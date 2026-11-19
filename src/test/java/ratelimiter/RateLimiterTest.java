package ratelimiter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class RateLimiterTest {

    private static final long MS = 1_000_000L; // nanoseconds in a millisecond
    private static final long SECOND = 1_000 * MS;

    /** A clock the tests move by hand. */
    private final AtomicLong now = new AtomicLong(0);

    private RateLimiter limiter(int limit) {
        return new RateLimiter(limit, now::get);
    }

    // ---- single thread -------------------------------------------------

    @Test
    void allowsUpToTheLimitThenRejects() {
        RateLimiter limiter = limiter(3);

        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
        assertFalse(limiter.allowRequest());
    }

    @Test
    void limitOfOneAllowsOneCallPerSecond() {
        RateLimiter limiter = limiter(1);

        assertTrue(limiter.allowRequest());
        assertFalse(limiter.allowRequest());

        now.addAndGet(999 * MS);
        assertFalse(limiter.allowRequest());

        now.addAndGet(1 * MS); // exactly one second after the first call
        assertTrue(limiter.allowRequest());
    }

    @Test
    void rejectsALimitThatIsNotPositive() {
        assertThrows(IllegalArgumentException.class, () -> new RateLimiter(0));
        assertThrows(IllegalArgumentException.class, () -> new RateLimiter(-5));
    }

    @Test
    void wholeWindowIsAvailableAgainAfterOneSecond() {
        RateLimiter limiter = limiter(5);
        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.allowRequest());
        }
        assertFalse(limiter.allowRequest());

        now.addAndGet(SECOND);

        for (int i = 0; i < 5; i++) {
            assertTrue(limiter.allowRequest());
        }
        assertFalse(limiter.allowRequest());
    }

    @Test
    void slotsFreeUpOneByOneAsOldCallsLeaveTheWindow() {
        RateLimiter limiter = limiter(3);

        assertTrue(limiter.allowRequest());   // t = 0 ms
        now.set(200 * MS);
        assertTrue(limiter.allowRequest());   // t = 200 ms
        now.set(400 * MS);
        assertTrue(limiter.allowRequest());   // t = 400 ms
        now.set(500 * MS);
        assertFalse(limiter.allowRequest());  // 3 calls inside the last second

        now.set(1000 * MS);
        assertTrue(limiter.allowRequest());   // the call at 0 ms just left the window
        now.set(1100 * MS);
        assertFalse(limiter.allowRequest());  // 200, 400, 1000 are still inside

        now.set(1200 * MS);
        assertTrue(limiter.allowRequest());   // the call at 200 ms just left the window
    }

    @Test
    void aRejectedCallDoesNotUseUpASlot() {
        RateLimiter limiter = limiter(2);

        assertTrue(limiter.allowRequest());   // t = 0
        now.set(100 * MS);
        assertTrue(limiter.allowRequest());   // t = 100 ms
        for (int i = 0; i < 50; i++) {
            assertFalse(limiter.allowRequest());
        }

        now.set(1000 * MS);                   // the first call has expired
        assertTrue(limiter.allowRequest());   // exactly one slot, so rejected calls were not counted
        assertFalse(limiter.allowRequest());
    }

    // ---- many threads --------------------------------------------------

    /**
     * Releases {@code threads} threads at the same moment, lets each make
     * {@code callsPerThread} calls, and returns how many were allowed.
     */
    private static int hammer(RateLimiter limiter, int threads, int callsPerThread) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();
        List<Future<?>> done = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            done.add(pool.submit(() -> {
                ready.countDown();
                go.await(); // every thread waits here, then they all start together
                for (int i = 0; i < callsPerThread; i++) {
                    if (limiter.allowRequest()) {
                        allowed.incrementAndGet();
                    }
                }
                return null;
            }));
        }

        assertTrue(ready.await(10, TimeUnit.SECONDS));
        go.countDown();
        for (Future<?> f : done) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();
        return allowed.get();
    }

    @Test
    void neverAllowsMoreThanTheLimitWhenManyThreadsCallAtOnce() throws Exception {
        RateLimiter limiter = limiter(10);

        // The clock is frozen, so all 10,000 calls fall inside the same second.
        int allowed = hammer(limiter, 100, 100);

        assertEquals(10, allowed);
    }

    @Test
    void stillExactAfterTheWindowMovesOn() throws Exception {
        RateLimiter limiter = limiter(10);

        assertEquals(10, hammer(limiter, 50, 200));
        now.addAndGet(SECOND);
        assertEquals(10, hammer(limiter, 50, 200));
        now.addAndGet(SECOND);
        assertEquals(10, hammer(limiter, 50, 200));
    }

    @Test
    void withTheRealClockNoMoreThanTwoWindowsWorthPassInOneAndAHalfSeconds() throws Exception {
        int limit = 50;
        RateLimiter limiter = new RateLimiter(limit);
        int threads = 32;
        long end = System.nanoTime() + 1_200 * MS;

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger allowed = new AtomicInteger();
        List<Future<?>> done = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            done.add(pool.submit(() -> {
                go.await();
                while (System.nanoTime() < end) {
                    if (limiter.allowRequest()) {
                        allowed.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        go.countDown();
        for (Future<?> f : done) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        // A 1.2 second run is covered by two one-second windows, each with at most `limit` calls.
        assertTrue(allowed.get() >= limit, "the first burst should be allowed, got " + allowed.get());
        assertTrue(allowed.get() <= 2 * limit, "too many calls allowed: " + allowed.get());
    }
}

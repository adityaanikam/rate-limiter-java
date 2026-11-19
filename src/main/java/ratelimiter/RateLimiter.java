package ratelimiter;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Allows at most {@code limit} calls in any one second, using a sliding window
 * of the times of recently allowed calls.
 */
public final class RateLimiter {

    private static final long WINDOW_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final int limit;
    private final LongSupplier nanoClock;
    private final Deque<Long> allowedAt = new ArrayDeque<>();

    public RateLimiter(int limit) {
        this(limit, System::nanoTime);
    }

    public RateLimiter(int limit, LongSupplier nanoClock) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive, was " + limit);
        }
        this.limit = limit;
        this.nanoClock = Objects.requireNonNull(nanoClock, "nanoClock");
    }

    // synchronized so the check and the update happen as one step
    public synchronized boolean allowRequest() {
        long now = nanoClock.getAsLong();

        // the subtraction stays correct even if nanoTime wraps around
        while (!allowedAt.isEmpty() && now - allowedAt.peekFirst() >= WINDOW_NANOS) {
            allowedAt.pollFirst();
        }

        if (allowedAt.size() < limit) {
            allowedAt.addLast(now);
            return true;
        }
        return false;
    }
}

# Rate limiter

A thread safe rate limiter for the sales channel API calls. `allowRequest()` returns `true` if the call may go ahead and `false` if it would put more than `N` calls inside one second. `N` is set when the limiter is created.

```java
RateLimiter limiter = new RateLimiter(100);   // at most 100 calls in any one second

if (limiter.allowRequest()) {
    callChannelApi();
}
```

Java 17, with no dependencies except JUnit 5 for the tests.

## Run it on your laptop

You need Java 17 or newer and Maven. Check them with `java -version` and `mvn -v`.

Run the tests:

```
mvn test
```

Try the limiter by hand in jshell, the Java shell that comes with the JDK:

```
mvn compile
jshell --class-path target/classes
```

Then type these lines in jshell:

```
var limiter = new ratelimiter.RateLimiter(3);
for (int i = 1; i <= 5; i++) System.out.println("call " + i + ": " + limiter.allowRequest());
Thread.sleep(1100);
System.out.println("after 1.1 seconds: " + limiter.allowRequest());
```

The loop makes five calls straight away. The first three print `true` and the next two print `false`, because the limit is 3 per second. After the pause the old calls have left the window, so the last line prints `true`. Type `/exit` to leave jshell.

Do not type `allowRequest()` by hand several times. Typing takes more than a second between calls, so each call is in a new window and they will all return `true`.

## How it works

I used a **sliding window log**. The limiter keeps the time of every call it allowed during the last second, oldest first, in a queue. On each call it:

1. reads the clock,
2. removes calls that are a full second old or older,
3. allows the new call only if fewer than `N` calls are left in the queue, and records its time.

A call that is rejected is not recorded, so a flood of rejected calls does not make things worse.

This gives the strict rule from the problem: in **any** one second span, at most `N` calls succeed, not just inside a fixed clock second.

## How I made it safe for many threads

The danger is a check then act race. If two threads both read "9 calls so far, limit is 10" and then both record a call, both are allowed and the limit is broken.

To prevent that, `allowRequest()` is `synchronized`, and all the state (the queue) is only touched inside it. Reading the clock, removing old calls, checking the count and recording the call happen as one step. Only one thread can be in that step at a time, so no two threads can take the last free slot.

Other choices that matter for correctness:

* The clock is read **inside** the lock, so the times in the queue are always in order, and removing old calls from the front is correct.
* I use `System.nanoTime()`, which is monotonic. `System.currentTimeMillis()` can jump if the system clock is changed. Ages are computed with a subtraction (`now - oldest >= 1s`), which stays correct even if `nanoTime` wraps around.
* Rejected calls never change the state.

## Alternatives I considered

| Algorithm | At most N in any one second? | Memory | Why I did not choose it |
|---|---|---|---|
| Sliding window log (my choice) | Yes | O(N) | Uses the most memory and needs a lock, but it is the only one here that always keeps the strict rule |
| Fixed window counter | No, up to 2N calls can pass across a window boundary | O(1) | Simplest and cheapest, but the boundary burst could get us blocked |
| Sliding window counter | No, it is an estimate | O(1) | Cheap, but it can let a few extra calls through |
| Token bucket | No, a burst of up to the bucket size plus the refill can pass | O(1) | Smooth on average and common in practice, but not a hard limit per second |
| Leaky bucket | Not a strict count per second | O(1) | Good for smoothing traffic, less suited to a hard limit |

Because the channel blocks us when we go over, I picked the option that never exceeds `N`, and accepted the extra memory. For thread safety I used one lock because it is simple and clearly correct. A lock free version (compare and set on an immutable state) is possible, but harder to get right.

## Tests

`RateLimiterTest` covers:

* single thread behaviour: the limit, a limit of one, an invalid limit, the full window resetting, and slots freeing one by one as old calls leave the window;
* **100 threads making 10,000 calls at the same moment** with a frozen clock: exactly `N` succeed, never more. This is repeated across window changes;
* a real clock test with 32 threads running for 1.2 seconds. Each thread records the time its own call was judged at. The test then sorts all the allowed calls and checks that no one second span contains more than `N` of them. This is a check on the real clock and real thread timing, next to the exact tests above.

The clock is passed in through a `LongSupplier`, so the tests that depend on time use a fake clock that they move by hand. This makes the boundary cases exact and means no test sleeps. The normal constructor uses `System.nanoTime()`. A custom clock must be monotonic, as `System.nanoTime()` is.

## What I was unsure about

* **What "a single second" means.** I read it as any one second span (a sliding window), which is the strictest reading. A fixed window (reset every clock second) is simpler and cheaper, but it can let through up to `2N` calls around a boundary, which would break "never more than `N`". If the channels only count calls per clock second, a fixed window would also be enough.
* **Exact boundary.** A call exactly one second old counts as expired. So the window is the span after `now - 1s` up to and including `now`.
* **Contention.** One lock means all threads queue up on it. Each call does only a little work inside the lock, so I expect this to be fine for API call rates, but I did not measure it with very high thread counts.
* **Memory.** The queue holds up to `N` timestamps, so it uses O(N) memory. This is fine for typical limits, but a very large `N` would use more.
* **One JVM only.** The limit is shared only between threads in one process. If several servers call the same channel, they would each allow `N`. Sharing the limit across servers would need a central store such as Redis, which I did not build.
* **Callers get only yes or no.** `allowRequest()` does not say how long to wait. A `retryAfter` value would be a useful addition.

## Layout

```
src/main/java/ratelimiter/RateLimiter.java        the limiter
src/test/java/ratelimiter/RateLimiterTest.java    the tests
```

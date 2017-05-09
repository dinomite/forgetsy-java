package net.dinomite.forgetsy

import org.junit.Assert.assertTrue
import java.time.Duration
import java.time.Instant

fun Int.days(): Duration {
    return Duration.ofDays(this.toLong())
}

fun Int.daysAgo(): Instant {
    return Instant.now().minus(Duration.ofDays(this.toLong()))
}

fun assertWithin(expected: Instant, actual: Instant, within: Duration = Duration.ofSeconds(1)) {
    assertTrue("\n$expected is too far from\n$actual",
            (expected.toEpochMilli() - actual.toEpochMilli()) < within.toMillis())
}

package net.dinomite.forgetsy

import java.time.Duration
import java.time.Instant

internal fun Instant.toTimestamp(): Double {
    return this.epochSecond.toDouble()
}

internal fun Duration.toDouble(): Double {
    return this.seconds.toDouble()
}
package net.dinomite.forgetsy

import redis.clients.jedis.JedisPool
import redis.clients.jedis.Tuple
import java.time.Duration
import java.time.Instant

/**
 * Ruby  | Java
 * -------------
 * date  | start
 * t     | lifetime
 *
 * @param jedisPool     A <a href="https://github.com/xetorthio/jedis">Jedis</a> pool instance
 * @param name          This delta's name
 * @param [lifetime]    Optional if Set exists in Redis, mean lifetime of an observation
 * @param [start]       Optional, time to begin the decay from
 */
open class Set(val jedisPool: JedisPool, val name: String, lifetime: Duration? = null, start: Instant? = null) {
    companion object {
        val LAST_DECAYED_KEY = "_last_decay"
        val LIFETIME_KEY = "_t"
        val SPECIAL_KEYS = listOf(LAST_DECAYED_KEY, LIFETIME_KEY)

        // Scrub keys scoring lower than this
        val HI_PASS_FILTER = "0.0001"
    }

    val lifetime: Duration
    val start: Instant

    init {
        if (lifetime == null && start == null) {
            // Attempt to lookup values
            try {
                this.lifetime = fetchLifetime()
                this.start = fetchLastDecayedDate()
            } catch (e: NullPointerException) {
                throw IllegalArgumentException("Set doesn't exist (pass lifetime to create it)")
            }
        } else if (lifetime != null) {
            // Create a new Set
            this.lifetime = lifetime
            this.start = start ?: Instant.now()

            updateDecayDate(this.start)
            jedisPool.resource.use { it.zadd(name, this.lifetime.toDouble(), LIFETIME_KEY) }
        } else {
            throw IllegalStateException("Must provide lifetime for new Set")
        }
    }

    fun fetch(num: Int = -1, decay: Boolean = true, scrub: Boolean = true): Map<String, Double> {
        if (decay) decayData()
        if (scrub) scrubData()

        return fetchRaw(num).associateBy({ it.element }, { it.score })
    }

    fun fetch(bin: String, decay: Boolean = true, scrub: Boolean = true): Map<String, Double> {
        if (decay) decayData()
        if (scrub) scrubData()

        return mapOf(bin to jedisPool.resource.use { it.zscore(name, bin) })
    }

    fun increment(bin: String, date: Instant = Instant.now()) {
        if (validIncrementDate(date)) {
            jedisPool.resource.use { it.zincrby(name, 1.toDouble(), bin) }
        }
    }

    fun incrementBy(bin: String, amount: Double, date: Instant = Instant.now()) {
        if (validIncrementDate(date)) {
            jedisPool.resource.use { it.zincrby(name, amount, bin) }
        }
    }

    /**
     * Apply exponential decay and update last decay time
     */
    internal fun decayData(date: Instant = Instant.now()) {
        val t0 = fetchLastDecayedDate().toTimestamp()
        val t1 = date.toTimestamp()
        val deltaT = t1 - t0

        val set = fetchRaw()
        val rate = 1 / fetchLifetime().toDouble()

        jedisPool.resource.pipelined().use { p ->
            set.forEach {
                val newValue = it.score * Math.exp(-deltaT * rate)
                p.zadd(name, newValue, it.element)
            }

            updateDecayDate(Instant.now())
            p.sync()
        }
    }

    /**
     * Scrub entries below threshold
     */
    internal fun scrubData() {
        jedisPool.resource.use { it.zremrangeByScore(name, "-inf", HI_PASS_FILTER) }
    }

    internal fun fetchLastDecayedDate(): Instant {
        return Instant.ofEpochSecond(jedisPool.resource.use { it.zscore(name, LAST_DECAYED_KEY) }.toLong())
    }

    internal fun fetchLifetime(): Duration {
        return Duration.ofSeconds(jedisPool.resource.use { it.zscore(name, LIFETIME_KEY) }.toLong())
    }

    internal fun fetchRaw(limit: Int = -1): List<Tuple> {
        val bufferedLimit = if (limit > 0) limit + SPECIAL_KEYS.size else limit

        val set = jedisPool.resource.use { it.zrevrangeWithScores(name, 0, bufferedLimit.toLong()) }
        return set.filter { !SPECIAL_KEYS.contains(it.element) }
    }

    internal fun updateDecayDate(date: Instant) {
        jedisPool.resource.use { it.zadd(name, date.toTimestamp(), LAST_DECAYED_KEY) }
    }

    internal fun validIncrementDate(date: Instant): Boolean {
        return date > fetchLastDecayedDate()
    }
}
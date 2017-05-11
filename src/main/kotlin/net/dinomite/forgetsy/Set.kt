package net.dinomite.forgetsy

import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Tuple
import java.time.Duration
import java.time.Instant

/**
 * @param jedisPool     A <a href="https://github.com/xetorthio/jedis">Jedis</a> pool instance
 * @param name          This delta's name
 * @param [lifetime]    Optional if Set exists in Redis, mean lifetime of an observation
 * @param [start]       Optional, time to begin the decay from
 */
open class Set(val jedisPool: JedisPool, val name: String, lifetime: Duration? = null, start: Instant? = null) {
    private val logger = LoggerFactory.getLogger(this.javaClass.name)

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
            logger.info("Reifying Delta $name")

            try {
                this.lifetime = fetchLifetime()
                this.start = fetchLastDecayedDate()
            } catch (e: NullPointerException) {
                throw IllegalStateException("Set doesn't exist (pass lifetime to create it)")
            }
        } else if (lifetime != null) {
            this.lifetime = lifetime
            this.start = start ?: Instant.now()

            logger.info("Creating new Set, $name, with lifetime $lifetime and start time $start")
            jedis {
                it.zadd(name, this.start.toTimestamp(), LAST_DECAYED_KEY)
                it.zadd(name, this.lifetime.toDouble(), LIFETIME_KEY)
            }
        } else {
            throw IllegalArgumentException("Must provide lifetime for new Set")
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
        logger.debug("Incrementing $bin at $date")
        if (validIncrementDate(date)) {
            jedis { it.zincrby(name, 1.toDouble(), bin) }
        }
    }

    fun incrementBy(bin: String, amount: Double, date: Instant = Instant.now()) {
        logger.debug("Incrementing $bin by $amount at $date")
        if (validIncrementDate(date)) {
            jedis { it.zincrby(name, amount, bin) }
        }
    }

    /**
     * Apply exponential decay and update last decay time
     */
    internal fun decayData(date: Instant = Instant.now()) {
        val t0 = fetchLastDecayedDate().toTimestamp()
        val t1 = date.toTimestamp()
        val deltaT = t1 - t0
        val rate = 1 / fetchLifetime().toDouble()
        val set = fetchRaw()

        logger.debug("Decaying $name. t0=$t0, t1=$t1, deltaT=$deltaT, rate=$rate")

        pipeline { p ->
            set.forEach {
                val newValue = it.score * Math.exp(-deltaT * rate)
                p.zadd(name, newValue, it.element)
            }

            logger.debug("Updating $name decay date to $date as part of decay")
            p.zadd(name, date.toTimestamp(), LAST_DECAYED_KEY)
        }
    }

    /**
     * Scrub entries below threshold
     */
    internal fun scrubData() {
        val count = jedis { it.zremrangeByScore(name, "-inf", HI_PASS_FILTER) }
        logger.debug("Scrubbed $count values from $name")
    }

    internal fun fetchLastDecayedDate(): Instant {
        return Instant.ofEpochSecond(jedis { it.zscore(name, LAST_DECAYED_KEY) }.toLong())
    }

    internal fun fetchLifetime(): Duration {
        return Duration.ofSeconds(jedis { it.zscore(name, LIFETIME_KEY) }.toLong())
    }

    internal fun fetchRaw(limit: Int = -1): List<Tuple> {
        val bufferedLimit = if (limit > 0) limit + SPECIAL_KEYS.size else limit

        val set = jedis { it.zrevrangeWithScores(name, 0, bufferedLimit.toLong()) }
        return set.filter { !SPECIAL_KEYS.contains(it.element) }
    }

    internal fun validIncrementDate(date: Instant): Boolean {
        return date > fetchLastDecayedDate()
    }

    private fun <T> jedis(body: (jedis: Jedis) -> T): T {
        return jedisPool.resource.use { body(it) }
    }

    private fun <T> pipeline(body: (pipeline: Pipeline) -> T): T {
        jedisPool.resource.use {
            it.pipelined().use {
                val ret = body(it)
                it.sync()
                return ret
            }
        }
    }
}
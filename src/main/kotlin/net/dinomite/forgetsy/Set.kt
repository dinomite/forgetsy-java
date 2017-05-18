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

        // Scrub keys scoring lower than this
        val HI_PASS_FILTER = 0.0001
    }

    val lifetime: Duration
    val start: Instant
    val lastDecayedKey = "$name$LAST_DECAYED_KEY"
    val lifetimeKey = "$name$LIFETIME_KEY"

    init {
        if (lifetime == null && start == null) {
            logger.info("Reifying Delta $name")

            try {
                this.lifetime = fetchLifetime()
                this.start = fetchLastDecayedDate()
            } catch (e: NumberFormatException) {
                if (e.message == "null") {
                    throw IllegalStateException("Set doesn't exist (pass lifetime to create it)")
                } else {
                    throw e
                }
            }
        } else if (lifetime != null) {
            this.lifetime = lifetime
            this.start = start ?: Instant.now()

            logger.info("Creating new Set, $name, with lifetime $lifetime and start time $start")
            pipeline {
                it.set(lastDecayedKey, this.start.epochSecond.toString())
                it.set(lifetimeKey, this.lifetime.seconds.toString())
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

    fun fetch(bin: String, decay: Boolean = true, scrub: Boolean = true): Double? {
        if (decay) decayData()
        if (scrub) scrubData()

        return jedis { it.zscore(name, bin) }
    }

    fun increment(bin: String, amount: Double = 1.0, date: Instant = Instant.now()) {
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
        val decayMultiplier = Math.exp(-deltaT * rate)
        val set = fetchRaw()

        logger.debug("Decaying $name. t0=$t0, t1=$t1, deltaT=$deltaT, rate=$rate")

        pipeline { p ->
            set.forEach {
                val newValue = it.score * decayMultiplier
                p.zadd(name, newValue, it.element)
            }

            logger.debug("Updating $name decay date to $date as part of decay")
            p.set(lastDecayedKey, date.epochSecond.toString())
        }
    }

    /**
     * Scrub entries below threshold
     */
    internal fun scrubData() {
        val count = jedis { it.zremrangeByScore(name, "-inf", "$HI_PASS_FILTER") }
        logger.debug("Scrubbed $count values from $name")
    }

    internal fun fetchLastDecayedDate(): Instant {
        return Instant.ofEpochSecond(jedis { it.get(lastDecayedKey) }.toLong())
    }

    internal fun fetchLifetime(): Duration {
        return Duration.ofSeconds(jedis { it.get(lifetimeKey) }.toLong())
    }

    internal fun fetchRaw(limit: Int = -1): MutableSet<Tuple> {
        return jedis { it.zrevrangeWithScores(name, 0, limit.toLong()) }
    }

    internal fun validIncrementDate(date: Instant): Boolean {
        return date > fetchLastDecayedDate()
    }

    private inline fun <T> jedis(body: (jedis: Jedis) -> T): T {
        return jedisPool.resource.use { body(it) }
    }

    private inline fun <T> pipeline(body: (pipeline: Pipeline) -> T): T {
        jedisPool.resource.use {
            it.pipelined().use {
                val ret = body(it)
                it.sync()
                return ret
            }
        }
    }
}
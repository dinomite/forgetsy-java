package net.dinomite.forgetsy

import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import java.time.Duration
import java.time.Instant

/**
 * @param jedisPool A <a href="https://github.com/xetorthio/jedis">Jedis</a> pool instance
 * @param name      This delta's name
 * @param lifetime  Optional if Delta already exists in Redis, mean lifetime of observation
 * @param [start]   Optional, an instant to start replaying from
 */
open class Delta(jedisPool: JedisPool, name: String, lifetime: Duration? = null, start: Instant? = null) {
    private val logger = LoggerFactory.getLogger(this.javaClass.name)

    companion object {
        val NORMAL_TIME_MULTIPLIER = 2
    }

    val primarySet: Set
    val secondarySet: Set

    init {
        val primaryKey: String = name
        val secondaryKey: String = "${name}_2t"

        if (lifetime == null) {
            logger.info("Reifying Delta $name")

            try {
                primarySet = Set(jedisPool, primaryKey)
                secondarySet = Set(jedisPool, secondaryKey)
            } catch (e: IllegalStateException) {
                throw IllegalStateException("Delta doesn't exist (pass lifetime to create it)")
            }
        } else {
            val now = Instant.now()
            val startTime = start ?: now.minus(lifetime)

            logger.info("Creating new Delta, $name, with lifetime $lifetime and start time $startTime")
            primarySet = Set(jedisPool, primaryKey, lifetime, startTime)

            // Secondary for retrospective observations
            val secondaryLifetime = Duration.ofSeconds(lifetime.seconds * NORMAL_TIME_MULTIPLIER)
            val secondaryStart = now.minus(Duration.ofSeconds(Duration.between(startTime, now).seconds * NORMAL_TIME_MULTIPLIER))

            logger.debug("Secondary set, $secondaryKey, with lifetime $lifetime and start time $secondaryStart")
            secondarySet = Set(jedisPool, secondaryKey, secondaryLifetime, secondaryStart)
        }
    }

    /**
     * Fetch the top bins
     *
     * @param   limit   The number of bins to fetch
     * @param   [decay] Optional, whether to decay before fetch
     * @param   [scrub] Optional, whether to scrube before fetch
     */
    fun fetch(limit: Int? = null, decay: Boolean = true, scrub: Boolean = true): Map<String, Double> {
        if (decay) decay()
        if (scrub) scrub()

        val counts = primarySet.fetch()
        val norm = secondarySet.fetch()

        val result: List<Pair<String, Double>> = counts.map {
            val normV = norm[it.key]
            val newValue = if (normV == null) 0.0 else it.value / normV
            it.key to newValue
        }

        val trim = if (limit != null && limit <= result.size) limit else result.size
        return result.subList(0, trim).toMap()
    }

    /**
     * Fetch an individual bin
     *
     * @param   bin     The bin to fetch
     * @param   [decay] Optional, whether to decay before fetch
     * @param   [scrub] Optional, whether to scrube before fetch
     */
    fun fetch(bin: String, decay: Boolean = true, scrub: Boolean = true): Map<String, Double?> {
        if (decay) decay()
        if (scrub) scrub()

        val counts = primarySet.fetch()
        val norm = secondarySet.fetch()

        val normV = norm[bin]
        if (normV == null) {
            return mapOf(bin to null)
        } else {
            return mapOf(bin to (counts[bin]!! / normV))
        }
    }

    /**
     * Decay the data.
     */
    fun decay() {
        primarySet.decayData()
        secondarySet.decayData()
    }

    /**
     * Scrub old entries
     */
    fun scrub() {
        primarySet.scrubData()
        secondarySet.scrubData()
    }

    /**
     * Increment a bin
     */
    fun increment(bin: String, amount: Double = 1.0) {
        logger.debug("Incrementing $bin by $amount")
        sets().forEach { it.increment(bin, amount) }
    }

    private fun sets() = listOf(primarySet, secondarySet)
}
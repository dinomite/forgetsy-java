package net.dinomite.forgetsy

import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.time.Duration
import java.time.Instant

class SetTest {
    var jedisPool: JedisPool

    lateinit var lifetime: Duration
    lateinit var set: Set

    val name = "foobar"
    val binName = "foo_bin"

    init {
        val port = 16379

        val redisServer: RedisServer = RedisServer(port)
        redisServer.start()

        jedisPool = JedisPool("localhost", port)
    }

    @Before
    fun setup() {
        jedisPool.resource.use { it.flushAll() }

        lifetime = 7.days()
        set = Set(jedisPool, name, lifetime)
    }

    @Test
    fun create() {
        jedisPool.resource.use {
            assertEquals("Creates Redis set with correct name & metadata", 2, it.zcount(name, "-inf", "+inf"))
        }

        assertEquals("Stores mean lifetime in special key when created", lifetime, set.fetchLifetime())
    }

    @Test
    fun create_withStartTime() {
        val start = 21.daysAgo()
        val set = Set(jedisPool, name, Duration.ofSeconds(300), start)

        assertWithin(start, set.fetchLastDecayedDate())
    }

    @Test
    fun create_reifiesExisting() {
        val sameSet = Set(jedisPool, name)

        val lifetime = set.fetchLifetime()
        val decayDate = set.fetchLastDecayedDate()
        assertEquals(lifetime, sameSet.fetchLifetime())
        assertEquals(decayDate, sameSet.fetchLastDecayedDate())
    }

    @Test
    fun create_failsForNonexistant() {
        try {
            Set(jedisPool, "does-not-exist")
            fail()
        } catch (e: IllegalArgumentException) {
            assertEquals("Set doesn't exist (pass lifetime to create it)", e.message)
        }

    }


    @Test
    fun increment() {
        set.increment(binName)
        jedisPool.resource.use { assertEquals("Increments counter correctly", 1.0, it.zscore(name, binName), .1) }
    }

    @Test
    fun incrementBatch() {
        set.incrementBy(binName, 5.0)
        jedisPool.resource.use { assertEquals("Increments in batches", 5.0, it.zscore(name, binName), .1) }
    }

    @Test
    fun increment_ignoresBadDate() {
        set.increment(binName, date = Instant.now().minus(lifetime.plusDays(5)))
        jedisPool.resource.use {
            assertNull("Ignores date older than last decay date", it.zscore(name, binName))
            assertNull("Fetch is null", set.fetch(bin = binName).values.first())
        }
    }


    @Test
    fun fetch_byBinName() {
        set.incrementBy(binName, 2.0)
        assertEquals(mapOf(binName to 2.0), set.fetch(bin = binName, decay = false))
    }

    @Test
    fun fetch_TopN() {
        val otherBin = "bar_bin"
        set.incrementBy(binName, 2.0)
        set.incrementBy(otherBin, 1.0)

        assertEquals(mapOf(binName to 2.0, otherBin to 1.0), set.fetch(2, false))
    }

    @Test
    fun fetch_All() {
        val otherBin = "bar_bin"
        set.incrementBy(binName, 2.0)
        set.incrementBy(otherBin, 1.0)

        assertEquals(mapOf(binName to 2.0, otherBin to 1.0), set.fetch(decay = false))
    }


    @Test
    fun decay() {
        val start = 2.daysAgo()
        val now = Instant.now()
        val delta = now.epochSecond - start.epochSecond
        val lifetime = 7.days()
        val rate = 1 / lifetime.toDouble()

        val fooName = "foo_bin"
        val fooValue = 2.0
        val barName = "bar_bin"
        val barValue = 10.0

        val set = Set(jedisPool, "decay_test", lifetime, start)
        set.incrementBy(fooName, fooValue)
        set.incrementBy(barName, barValue)

        val decayedFoo = fooValue * Math.exp(- rate * delta)
        val decayedBar = barValue * Math.exp(- rate * delta)

        set.decayData(now)
        assertEquals(decayedFoo, set.fetch(bin = fooName).values.first(), .1)
        assertEquals(decayedBar, set.fetch(bin = barName).values.first(), .1)
    }

    @Test
    fun scrub() {
        val start = 365.daysAgo()
        val lifetime = 7.days()
        val set = Set(jedisPool, "decay_test", lifetime, start)

        set.increment(binName)

        assertEquals(0, set.fetch().values.size)
    }


    private fun Int.days(): Duration {
        return Duration.ofDays(this.toLong())
    }

    private fun Int.daysAgo(): Instant {
        return Instant.now().minus(Duration.ofDays(this.toLong()))
    }

    private fun assertWithin(expected: Instant, actual: Instant, within: Duration = Duration.ofSeconds(1)) {
        assertTrue("\n$expected is too far from\n$actual",
                (expected.toEpochMilli() - actual.toEpochMilli()) < within.toMillis())
    }
}
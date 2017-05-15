package net.dinomite.forgetsy

import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import redis.clients.jedis.JedisPool
import redis.embedded.RedisServer
import java.time.Duration

class DeltaTest {
    var jedisPool: JedisPool

    lateinit var lifetime: Duration
    lateinit var delta: Delta

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
        delta = Delta(jedisPool, name, lifetime)
    }

    @Test
    fun create() {
        jedisPool.resource.use {
            assertNotNull("Creates primary Set", it.get(delta.primarySet.lifetimeKey))
            assertNotNull("Creates secondary Set", it.get(delta.secondarySet.lifetimeKey))
            assertEquals(4, it.dbSize())
        }
    }

    @Test
    fun create_reifiesExisting() {
        Delta(jedisPool, name)

        jedisPool.resource.use { assertEquals(4, it.dbSize()) }
    }

    @Test
    fun create_failsForNonExistent() {
        try {
            Delta(jedisPool, "does-not-exist")
            fail()
        } catch (e: IllegalStateException) {
            assertEquals("Delta doesn't exist (pass lifetime to create it)", e.message)
        }
    }

    @Test
    fun create_retrospective() {
        val retrospective = Delta(jedisPool, name, 7.days())

        val secondaryDecayDate = retrospective.secondarySet.fetchLastDecayedDate()
        val primaryDecayDate = retrospective.primarySet.fetchLastDecayedDate()
        assertTrue("Secondary start is before primary", secondaryDecayDate < primaryDecayDate)

        retrospective.increment("UserFoo", date = 14.daysAgo())
        retrospective.increment("UserBar", date = 10.daysAgo())
        retrospective.increment("UserBar", date = 7.daysAgo())
        retrospective.increment("UserFoo", date = 1.daysAgo())
        retrospective.increment("UserFoo")

        val values = retrospective.fetch()
        assertEquals(0.67, values["UserFoo"]!!, 0.01)
        assertEquals(0.50, values["UserBar"]!!, 0.01)
    }


    @Test
    fun fetch_normalizedCountsForAllScores() {
        delta.increment(binName)
        delta.increment(binName)
        delta.increment("bar_bin")

        val scores = delta.fetch()
        assertEquals(mapOf(binName to 1.0, "bar_bin" to 1.0), scores)
    }

    @Test
    fun fetch_limitsResults() {
        delta.increment(binName)
        delta.increment("bar_bin")
        delta.increment("baz_bin")

        assertEquals(2, delta.fetch(2).size)
        assertEquals(3, delta.fetch(3).size)
        assertEquals(3, delta.fetch(4).size)
    }

    @Test
    fun fetch_normalizedCountsForSingleBin() {
        delta.increment(binName)
        delta.increment(binName)
        delta.increment("bar_bin")

        assertEquals(1.0, delta.fetch(binName).values.first()!!, .01)
        assertEquals(1.0, delta.fetch("bar_bin").values.first()!!, .01)
    }

    @Test
    fun fetch_nilForNonexistentBin() {
        val bin = "does-not-exist"
        assertEquals(mapOf<String, Double?>(bin to null), delta.fetch(bin))
    }
}
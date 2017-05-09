package net.dinomite.forgetsy

import redis.clients.jedis.JedisPool
import java.time.Duration

/**
 * @param jedisPool     A <a href="https://github.com/xetorthio/jedis">Jedis</a> pool instance
 * @param name          This delta's name
 * @param [multiplier]  Optional, the time multiplier to use for this delta
 */
class Delta(val jedisPool: JedisPool, val name: String, val multiplier: Duration = Duration.ofSeconds(2)) {
    init {

    }
}
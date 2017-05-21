# forgetsy-jvm

A JVM port of [Forgetsy](https://github.com/cavvia/forgetsy) in Kotlin.  Data is stored in Redis, so you'll need one of those.

# Synopsis

```kotlin
...
jedisPool = JedisPool(redisHost, redisPort)

val followsData = Delta(jedisPool, name, 7.days(), true)

followsData.increment("UserFoo", date = 14.daysAgo())
followsData.increment("UserBar", date = 10.daysAgo())
followsData.increment("UserBar", date = 7.daysAgo())
followsData.increment("UserFoo", date = 1.daysAgo())
followsData.increment("UserFoo")

println(followsData.fetch())
```

Outputs:

```{UserFoo=0.6666666666666666, UserBar=0.5}```

# Usage

forgetsy-jvm is distributed included in [jcenter](https://bintray.com/bintray/jcenter).  If you're using Gradle, add
`jcenter()` to your repositories block:

    repositories {
        jcenter()
    }

If you use Maven, rethink your life choices (Maven is fine to keep using, but for new projects it's worth [learning Gradle](https://bitbucket.org/marshallpierce/java-quickstart)):

    <repositories>
        <repository>
          <id>jcenter</id>
          <url>https://jcenter.bintray.com/</url>
        </repository>
    </repositories>

Then just add the dependency.

In Gradle:

    compile 'net.dinomite:forgetsy:0.3.3'

In Maven (ick):

    <dependency>
      <groupId>net.dinomite</groupId>
      <artifactId>forgetsy</artifactId>
      <version>0.3.3</version>
      <type>pom</type>
    </dependency>

## Delta creation

[`Delta`](https://github.com/dinomite/forgetsy-jvm/blob/master/src/main/kotlin/net/dinomite/forgetsy/Delta.kt) will
attempt to use existing data in Redis if you don't provide a lifetime for and the underlying Sorted Sets exist,
throwing `IllegalStateException` if the data isn't found.  If you want to re-use existing data upon restart of your app,
but create a new one if necessary, do something like this:

```kotlin
var delta = reifyOrMakeDelta(jedisPool)

fun reifyOrMakeDelta(jedisPool: JedisPool): Delta {
    try {
        return Delta(jedisPool, "namesy")
    } catch (e: IllegalStateException) {
        return Delta(jedisPool, "namesy", Duration.ofDays(10))
    }
}
```

## Incrementing

The most common interaction with your delta will be incrementing bins:

```kotlin
delta.increment("$binName")
```

## Fetching

Use the `fetch()` method to get the stats about what is trending:

```kotlin
delta.fetch()
```

Deltas are likely to contain more bins than you want to see.  Pass a count to fetch to get the top n buckets:

```kotlin
delta.fetch(25)
```

# Kotlin (and Java)

This library is written in Kotlin and I use it from Kotlin.  Kotlin has great Java interop…but this library makes use of
named arguments which Java doesn't support and likely makes the API as viewed from Java pretty janky.  If you want to
use this from Java provide a PR or just let me know and we can work on it together.

# See also

- [Forgetsy](https://github.com/cavvia/forgetsy)
- [bitly's post on Forget Table](http://word.bitly.com/post/41284219720/forget-table)
- [bitly's Forget Table source](https://github.com/bitly/forgettable)

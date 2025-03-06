package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val queue = LinkedBlockingQueue<Unit>(bucketSize)

    override fun tick(): Boolean {
        return queue.offer(Unit)
    }

    suspend fun tickBlocking(): Boolean {
        if (!queue.offer(Unit)) {
            return false
        }
        delay(window.toMillis() / rate * queue.size)
        return true
    }

    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            delay(window.toMillis() / rate)
            queue.poll()
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }

    class WaitCondition(var value: Boolean)
}
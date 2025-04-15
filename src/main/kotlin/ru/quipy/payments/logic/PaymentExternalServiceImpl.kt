package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.SocketTimeoutException
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.time.Duration.ofSeconds
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.math.abs
import kotlin.math.min

//{
//    "ratePerSecond": 7,
//    "testCount": 800,
//    "processingTimeMillis": 3500
//}

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
        const val MAX_RETRY_COUNT = 4
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val requestQueue = ConcurrentLinkedQueue<PaymentRequest>()
    // private val executor = Executors.newFixedThreadPool(parallelRequests)

    // private val limiter = TokenBucketRateLimiter(
    //     rate = rateLimitPerSec,
    //     window = 1005,
    //     bucketMaxCapacity = rateLimitPerSec,
    //     timeUnit = TimeUnit.MILLISECONDS
    // )

    private val limiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000))

    private val semaphore = Semaphore(parallelRequests, true)

    private val requestTimeout = Duration.ofMillis(requestAverageProcessingTime.toMillis() * 2)
    private val client = HttpClient.newBuilder()
         .connectTimeout(requestTimeout)
         .version(HttpClient.Version.HTTP_1_1)
        .build()

    // override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
    //     logger.warn("[$accountName] Adding payment request for payment $paymentId to queue")
    //     requestQueue.add(PaymentRequest(paymentId, amount, paymentStartedAt, deadline))
    //     processQueue()
    // }
    //
    // private fun processQueue() {
    //     while (requestQueue.isNotEmpty()) {
    //         val request = requestQueue.poll() ?: return
    //         // executor.submit {
    //             executePayment(request, 0)
    //         // }
    //     }
    // }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        executePayment(PaymentRequest(paymentId, amount, paymentStartedAt, deadline), 0)
    }

    private fun executePayment(request: PaymentRequest, attempt: Int) {
        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Processing payment for ${request.paymentId}, txId: $transactionId")

        paymentESService.update(request.paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - request.paymentStartedAt))
        }

        if (!limiter.tick() || !semaphore.tryAcquire()) {
            CompletableFuture.delayedExecutor(1, TimeUnit.MILLISECONDS).execute {
                executePayment(request, attempt)
            }
            return
        }

        // limiter.tickBlocking()
        val httpRequest = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=${request.paymentId}&amount=${request.amount}&timeout=${requestTimeout}"))
            .timeout(requestTimeout)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        // try {
            client.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString()).whenComplete { response, ex ->

                if (ex != null) {
                    val reason = when (ex) {
                        is HttpTimeoutException -> "Request timeout"
                        else -> ex.message ?: "Unknown error"
                    }

                    logger.error("[$accountName] Payment failed: $reason", ex)
                    paymentESService.update(request.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason)
                    }

                    if (attempt < MAX_RETRY_COUNT) {
                        executePayment(request, attempt + 1)
                    }

                    return@whenComplete
                }

                // val responseBody = response.body()
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: ${request.paymentId}", e)
                    ExternalSysResponse(transactionId.toString(), request.paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${request.paymentId}, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(request.paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                if (!body.result && attempt < MAX_RETRY_COUNT) {
                    executePayment(request, attempt + 1)
                }
            }
        // } finally {
        //     processQueue()
        // }

        paymentESService.update(request.paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Max retries reached")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private data class PaymentRequest(
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val deadline: Long
)

public fun now() = System.currentTimeMillis()
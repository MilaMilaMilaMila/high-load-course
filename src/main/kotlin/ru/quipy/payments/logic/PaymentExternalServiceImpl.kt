package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val requestTimeout = requestAverageProcessingTime.multipliedBy(2)
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = HttpClient.newBuilder()
        // .connectTimeout(requestTimeout)
        .version(HttpClient.Version.HTTP_2)
        .build()

    private val rateLimiter = TokenBucketRateLimiter(
        rate = rateLimitPerSec,
        window = 1005,
        bucketMaxCapacity = rateLimitPerSec,
        timeUnit = TimeUnit.MILLISECONDS
    )

    private val semaphore = Semaphore(parallelRequests, true)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val uri = URI.create(
            "http://localhost:1234/external/process?serviceName=$serviceName&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
        )

        val request = HttpRequest.newBuilder()
            .uri(uri)
            .timeout(requestTimeout)
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).whenComplete { response, ex ->
            if (ex != null) {
                val reason = when (ex) {
                    is HttpTimeoutException -> "Request timeout"
                    else -> ex.message ?: "Unknown error"
                }

                logger.error("[$accountName] Payment failed: $reason", ex)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason)
                }
                return@whenComplete
            }

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] Failed to parse response: ${response.body()}", e)
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Parse error")
            }

            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, body.message)
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
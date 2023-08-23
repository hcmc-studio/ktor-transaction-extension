package studio.hcmc.ktor.transaction

import io.ktor.server.application.*
import io.ktor.util.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import studio.hcmc.exposed.transaction.Transaction
import studio.hcmc.kotlin.coroutines.launch
import java.time.Duration
import java.util.*

class TransactionStateConsumerConfig(
    /**
     * Kafka 서버
     */
    var url: List<String> = listOf(),
    /**
     * Consumer 그룹 ID
     */
    var groupId: String = "",
    /**
     * Consumer 구독 topics
     */
    var topics: List<String> = listOf(),
    /**
     * 처리할 record key prefixes
     */
    var keyPrefixes: List<String> = listOf(),
    /**
     * polled record가 비었을 때 대기 시간
     */
    var emptyDelayMillis: Long = 1000,
    /**
     * commit 또는 rollback할 트랜잭션의 ID를 포함할 record header name
     */
    var transactionIdHeaderName: String = "x-hcmc-transaction-id",
    /**
     * 로그 수집
     */
    var logger: Logger = LoggerFactory.getLogger("TransactionStateConsumer")
) {
}

internal val transactions = HashMap<UUID, Pair<Int, Channel<TransactionState>>>()
internal val transactionLock = Mutex()
internal val transactionIdHeaderNameKey = AttributeKey<String>("TransactionIdHeaderName")

val TransactionStateConsumer = createApplicationPlugin("TransactionStateConsumer", ::TransactionStateConsumerConfig) {
    if (pluginConfig.url.isEmpty()) {
        throw IllegalArgumentException("config.url is empty.")
    }
    if (pluginConfig.topics.isEmpty()) {
        throw IllegalArgumentException("config.topics is empty.")
    }
    if (pluginConfig.keyPrefixes.isEmpty()) {
        throw IllegalArgumentException("config.keyPrefixes is empty.")
    }

    application.attributes.put(transactionIdHeaderNameKey, pluginConfig.transactionIdHeaderName)

    val producer: KafkaProducer<String, TransactionState> = KafkaProducer(
        Properties().apply {
            setProperty("bootstrap.servers", pluginConfig.url.joinToString(","))
        },
        StringSerializer(),
        TransactionState.KafkaSerializer
    )
    val consumer: KafkaConsumer<String, TransactionState> = KafkaConsumer(
        Properties().apply {
            setProperty("bootstrap.servers", pluginConfig.url.joinToString(","))
            setProperty("group.id", pluginConfig.groupId)
        },
        StringDeserializer(),
        TransactionState.KafkaDeserializer
    )

    newSingleThreadContext("TransactionStateConsumer").launch {
        consumer.subscribe(pluginConfig.topics)

        while (true) {
            val records = consumer.poll(Duration.ZERO)
            if (records.isEmpty) {
                delay(pluginConfig.emptyDelayMillis)
                continue
            }

            for (record in records) {
                onEachRecord(producer, record)
            }
        }
    }
}

private suspend fun PluginBuilder<TransactionStateConsumerConfig>.onEachRecord(
    producer: KafkaProducer<String, TransactionState>,
    record: ConsumerRecord<String, TransactionState>
) {
    val transactionId = record.headers()
        .associateBy { it.key() }[pluginConfig.transactionIdHeaderName]
        ?.let { UUID.fromString(String(it.value())) } ?: return

    val key = record.key()
    for (keyPrefix in pluginConfig.keyPrefixes) {
        if (key.startsWith(keyPrefix)) {
            val (count, channel) = withContext(Dispatchers.Transaction) {
                transactionLock.withLock { transactions.remove(transactionId) }
            } ?: continue

            repeat(count) {
                channel.send(record.value())
            }

            return
        }
    }

    pluginConfig.logger.warn("No matching prefix for transaction $transactionId, re-sending record: key=${record.key()}, value=${record.value()}")
    producer.send(ProducerRecord(
        record.topic(),
        record.partition(),
        record.timestamp(),
        record.key(),
        record.value(),
        record.headers()
    ))
}
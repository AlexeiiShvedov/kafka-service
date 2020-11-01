/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.icerockdev.service.kafka.KafkaConsumerBuilder
import com.icerockdev.service.kafka.KafkaConsumerExecutionPool
import com.icerockdev.service.kafka.KafkaProducerBuilder
import com.icerockdev.service.kafka.KafkaSender
import com.icerockdev.service.kafka.ObjectDeserializer
import com.icerockdev.service.kafka.ObjectSerializer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.time.Duration
import kotlin.test.assertEquals

class KafkaTest {
    private val hostname: String = "hostname"//java.net.InetAddress.getLocalHost().hostName

    // specify unique client id by hostname and app name
    private val clientId = "app-name::${hostname}"

    private val servers = "localhost:9092"
    private val groupId = "group"
    private val anotherGroupId = "anotherGroup"
    private val topic = "topic"
    private val pollTimeout = Duration.ofSeconds(1)

    @Before
    fun init() {
        producer = KafkaProducerBuilder()
//        .applyTransactional(KAFKA_TRANSACTION_ID) // supported only for 3 brokers and more
                .applyIdempotence()
                .applyTimeout()
                .applyBuffering()
                .build<Long, String>(
                        servers = servers,
                        clientId = clientId,
                        keySerializer = LongSerializer(),
                        valueSerializer = ObjectSerializer()
                )

        consumer =
                KafkaConsumerBuilder()
                        .applyReadOpt()
                        .applyIsolation(KafkaConsumerBuilder.IsolationLevel.READ_COMMITTED)
                        .apply {
                            with(props) {
                                this[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = 50 * 1024 * 1024
                                this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = KafkaConsumerBuilder.Offset.LATEST.value
                            }
                        }
                        .build<Long, String>(
                                servers,
                                groupId,
                                clientId,
                                LongDeserializer(),
                                ObjectDeserializer(jacksonTypeRef())
                        )

        // Read any old messages (actual for crushed test)
        consumer.subscribe(listOf(topic))
        val cnt = consumer.poll(pollTimeout)
        println("Init consumer read: $cnt")
        consumer.commitSync()
        consumer.unsubscribe()
    }

    @Test
    fun testBasic() {
        consumer.subscribe(listOf(topic))
        KafkaSender.send(producer = producer, topic = topic, key = System.currentTimeMillis(), value = "Sync sent value 1")

        val records = consumer.poll(pollTimeout)
        assertEquals(1, records.count())

        KafkaSender.send(producer = producer, topic = topic, key = System.currentTimeMillis(), value = "Sync sent value 2")

        // rewind uncommitted reading
        records.forEach {
            consumer.seek(TopicPartition(it.topic(), it.partition()), it.offset())
        }
        // try next read without commit
        val rereadRecords = consumer.poll(pollTimeout)
        assertEquals(2, rereadRecords.count())
        consumer.commitSync()

        val rereadRecords2 = consumer.poll(pollTimeout)
        assertEquals(0, rereadRecords2.count())
    }

    @Test
    fun testExecutor() {
        (1..10).forEach {
            KafkaSender.sendAsync(producer = producer, topic = topic, key = System.currentTimeMillis(), value = "Async sent value $it")
        }
        producer.flush()

        val executor = KafkaConsumerExecutionPool(CoroutineScope(Dispatchers.IO + SupervisorJob()))

        var cnt = 0
        executor.runExecutor(consumer = consumer, topicList = listOf(topic)) {
            cnt++
            println("Process: ${value()}")
            // Test reread wrong messages
            if (cnt == 5) {
                return@runExecutor false
            }
            true
        }

        Thread.sleep(500)
        executor.close()
        assertEquals(11, cnt)
    }


    @After
    fun close() {
        producer.flush()
        producer.close()
        consumer.close()
    }

    companion object {
        private lateinit var producer: KafkaProducer<Long, String>
        private lateinit var consumer: KafkaConsumer<Long, String>
    }
}

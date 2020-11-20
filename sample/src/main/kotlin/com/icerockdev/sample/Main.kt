/*
 * Copyright 2019 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.sample

import com.fasterxml.jackson.module.kotlin.jacksonTypeRef
import com.icerockdev.service.kafka.IKafkaConsumer
import com.icerockdev.service.kafka.consumer.KafkaConsumerConfig
import com.icerockdev.service.kafka.KafkaConsumerExecutionPool
import com.icerockdev.service.kafka.KafkaProducerBuilder
import com.icerockdev.service.kafka.KafkaSender
import com.icerockdev.service.kafka.ObjectDeserializer
import com.icerockdev.service.kafka.ObjectSerializer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.net.InetAddress
import java.time.Duration
import java.util.Collections

object Main {
    @JvmStatic
    fun main(args: Array<String>) {
        val hostname: String = InetAddress.getLocalHost().hostName
        // specify unique client id by hostname and app name
        val clientId = "app-name::${hostname}"

        val servers = "localhost:9092"
        val groupId = "group"
        val topic = "topic"

        val service = TestProducerService(servers, clientId, topic)
        val consumer = TestKafkaConsumer(servers, groupId, clientId)
        val executor = KafkaConsumerExecutionPool(CoroutineScope(Dispatchers.IO + SupervisorJob()))
        consumer.run(executor, topic)

        for (i in 1..9) {
            service.sendAsyncData("Sanded value: $i")
        }
        for (i in 10..20) {
            service.sendData("Sanded value: $i")
        }

        service.close()

        Thread.sleep(1000)

        println("Shutdown started")
        executor.close()
        consumer.close()
    }
}

class TestProducerService(servers: String, clientId: String, private val topic: String) : AutoCloseable {
    private val producer = KafkaProducerBuilder()
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

    fun sendData(model: String): Boolean {
        val time = System.currentTimeMillis()
        return KafkaSender.send(producer, topic, time, model)
    }

    fun sendAsyncData(model: String) {
        val time = System.currentTimeMillis()
        KafkaSender.sendAsync(producer, topic, time, model)
    }

    override fun close() {
        producer.flush()
        producer.close()
    }
}

class TestKafkaConsumer(servers: String, groupId: String, clientId: String) : IKafkaConsumer {

    private val consumer =
        KafkaConsumerConfig()
            .applyReadOpt()
            .applyIsolation(KafkaConsumerConfig.IsolationLevel.READ_COMMITTED)
            .apply {
                with(props) {
                    this[ConsumerConfig.FETCH_MAX_BYTES_CONFIG] = 50 * 1024 * 1024
                    this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = KafkaConsumerConfig.Offset.LATEST.value
                }
            }
            .applyConfig<String, String>(
                servers,
                groupId,
                clientId,
                StringDeserializer(),
                ObjectDeserializer(jacksonTypeRef())
            )


    override fun run(executor: KafkaConsumerExecutionPool, topic: String) {
        executor.runExecutor(
            consumer = consumer,
            topicList = Collections.singletonList(topic),
            pollWait = Duration.ofMillis(100)
        ) {
            println("Read value: ${value()}")
            true
        }

    }

    override fun close() {
        consumer.close()
    }
}
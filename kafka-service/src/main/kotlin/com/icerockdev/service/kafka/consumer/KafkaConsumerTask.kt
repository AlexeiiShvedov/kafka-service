/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

import com.icerockdev.service.kafka.KafkaConsumerExecutionPool
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class KafkaConsumerTask<K, V>(
        config: KafkaConsumerConfig,
        factory: IConsumerFactory,
        clientId: String,
        private val pollWait: Duration,
        private val commitCallback: (MutableMap<TopicPartition, OffsetAndMetadata>, Exception?) -> Unit,
        private val block: suspend ConsumerRecord<K, V>.() -> Boolean
) : IConsumerTask {
    private val consumer: Consumer<K, V>
    private val isRunning = AtomicBoolean(false)


    init {
        // clone before modification
        val internalConfig = Properties(config)
        internalConfig[ConsumerConfig.CLIENT_ID_CONFIG] = clientId

        consumer = factory.make(internalConfig)

    }

    // TODO: autocommit read from config! Move error handling
    // TODO: think about parallel processing of batch
    override suspend fun execute() {
        while (isRunning.get()) {
            try {
                val consumerRecords = consumer.poll(pollWait)
                if (consumerRecords.count() == 0) {
                    continue
                }

                withContext(NonCancellable) {
                    val map: MutableMap<TopicPartition, OffsetAndMetadata> = HashMap()
                    for (record in consumerRecords) {
                        val result = try {
                            record.block()
                        } catch (e: Throwable) {
                            KafkaConsumerExecutionPool.logger.error(e.localizedMessage, e)
                            false
                        }
                        if (result) {
                            val topicKey = TopicPartition(record.topic(), record.partition())
                            // offset in previous record always lower than current
                            map[topicKey] = OffsetAndMetadata(record.offset())
                        } else {
                            TopicPartition(record.topic(), record.partition())
                            record.offset()
                            consumer.seek(TopicPartition(record.topic(), record.partition()), record.offset())
                            break
                        }
                    }
                    if (map.isNotEmpty()) {
                        consumer.commitAsync(map, commitCallback)
                    }
                }

            } catch (e: Throwable) {
                logger.error(e.localizedMessage, e)
                continue
            }
        }
    }

    override fun pause() {
        val topicPartitionSet = consumer.assignment()
        consumer.pause(topicPartitionSet)
    }

    // TODO: partial pause and resume?
    override fun resume() {
        val topicPartitionSet = consumer.assignment()
        consumer.resume(topicPartitionSet)
    }

    override fun close() {
        // TODO: await task and await by timeout
        consumer.close()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaConsumerTask::class.java)
    }
}

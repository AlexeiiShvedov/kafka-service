/*
 * Copyright 2019 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Duration

class KafkaConsumerExecutionPool(scope: CoroutineScope) : AutoCloseable, CoroutineScope by scope {
    private val job = Job()

    fun <K, V> runExecutor(
            consumer: KafkaConsumer<K, V>,
            topicList: List<String>,
            pollWait: Duration = Duration.ofMillis(100),
            commitCallback: (MutableMap<TopicPartition, OffsetAndMetadata>, Exception?) -> Unit = { _: MutableMap<TopicPartition, OffsetAndMetadata>, _: Exception? -> },
            block: suspend ConsumerRecord<K, V>.() -> Boolean
    ) {
        consumer.subscribe(topicList)
        logger.info("Consumer subscribed: $topicList")

        launch(job) {
            while (isActive) {
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
                                logger.error(e.localizedMessage, e)
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
            logger.info("Consumer executor closed: $topicList")
        }
    }

    /**
     * Waiting all coroutine
     */
    override fun close() {
        runBlocking {
            job.cancelAndJoin()
        }
        cancel()
        logger.info("KafkaConsumerExecutionPool stopped")
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KafkaConsumerExecutionPool::class.java)
    }
}
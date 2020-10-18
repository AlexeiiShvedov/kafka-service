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
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration

class KafkaConsumerExecutionPool(scope: CoroutineScope) : AutoCloseable, CoroutineScope by scope {
    val job = Job()

    inline fun <reified V> runExecutor(
            consumer: KafkaConsumer<String, V>,
            topicList: List<String>,
            pollWait: Duration = Duration.ofMillis(100),
            crossinline block: suspend ConsumerRecords<String, V>.() -> Boolean
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

                    if (withContext(NonCancellable) { consumerRecords.block() }) {
                        consumer.commitAsync()
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
/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock

class KafkaConsumerExecutor<K, V>(
        private val config: KafkaConsumerConfig,
        private val consumerFactory: IConsumerFactory = IConsumerFactory.DefaultConsumerFactory,
        private val pollWait: Duration = Duration.ofMillis(100),
        private val commitCallback: (MutableMap<TopicPartition, OffsetAndMetadata>, Exception?) -> Unit = { _: MutableMap<TopicPartition, OffsetAndMetadata>, _: Exception? -> },
        private val block: suspend ConsumerRecord<K, V>.() -> Boolean
) : IConsumerExecutor<K, V> {

    private var taskList: List<KafkaConsumerTask<K, V>> = mutableListOf()
    private var consumerJobs: List<Job> = mutableListOf()
    private val defaultClientIdPrefix = "consumer-${config[ConsumerConfig.GROUP_ID_CONFIG]}"

    private val lock = ReentrantLock()
    private var isStarted = false
    private var scope: CoroutineScope? = null

    override fun start(topics: List<String>, threadCount: Int): Boolean {
        if (isStarted) {
            return false
        }

        lock.lock()
        try {
            if (isStarted) {
                return false
            }
            taskList = (1..threadCount).map {
                KafkaConsumerTask(
                        config = config,
                        factory = consumerFactory,
                        clientId = getClientId(it),
                        pollWait = pollWait,
                        commitCallback = commitCallback,
                        block = block
                )
            }

            val threadNumber = AtomicInteger(1)
            val executor = Executors.newFixedThreadPool(threadCount) {
                Thread(it, "com.icerockdev.service.kafka.consumer-${config.groupId}-${threadNumber.getAndIncrement()}").also { t -> t.isDaemon = true }
            }

            // TODO: check correct executor shutdown
            scope = CoroutineScope(executor.asCoroutineDispatcher())
            consumerJobs = taskList.map {
                scope!!.launch {
                    it.execute()
                }
            }
            isStarted = true
            return true
        } finally {
            lock.unlock()
        }
    }

    override fun pause(): Boolean {
        if (!isStarted) {
            return false
        }
        lock.lock()
        try {
            if (!isStarted) {
                return false
            }
            taskList.map {
                it.pause()
            }

            return true
        } finally {
            lock.unlock()
        }
    }

    override fun resume(): Boolean {
        if (!isStarted) {
            return false
        }
        lock.lock()
        try {
            if (!isStarted) {
                return false
            }
            taskList.map {
                it.resume()
            }

            return true
        } finally {
            lock.unlock()
        }
    }

    override fun close() {
        if (!isStarted) {
            return
        }
        lock.lock()
        try {
            if (!isStarted) {
                return
            }
            taskList.map {
                it.close()
            }
            scope?.cancel()
            isStarted = false
        } finally {
            lock.unlock()
        }
    }

    /**
     * Append taskId suffix or generate clientId (if absent)
     */
    private fun getClientId(taskId: Int): String {
        val clientId = config[CommonClientConfigs.CLIENT_ID_CONFIG]
        return clientId?.let { "$clientId-$taskId" } ?: "$defaultClientIdPrefix-$taskId"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(KafkaConsumerExecutor::class.java)
    }
}
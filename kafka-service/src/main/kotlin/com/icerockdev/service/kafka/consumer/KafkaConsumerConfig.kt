/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Deserializer
import java.util.Properties

class KafkaConsumerConfig: Properties() {
    enum class Offset(val value: String) {
        LATEST("latest"),
        EARLIEST("earliest")
    }

    enum class IsolationLevel(val value: String) {
        READ_UNCOMMITTED("read_uncommitted"),
        READ_COMMITTED("read_committed")
    }

    val groupId: String
        get() = this[ConsumerConfig.GROUP_ID_CONFIG].toString()

    fun applyReadOpt(
            maxPollRecords: Int = 50,
            resetOffset: Offset = Offset.LATEST,
            enableAutocommit: Boolean = false,
            autocommitIntervalMs: Int = 5000
    ): KafkaConsumerConfig {
        this[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = maxPollRecords
        this[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = resetOffset.value
        this[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = enableAutocommit
        this[ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG] = autocommitIntervalMs
        return this
    }

    fun applyIsolation(level: IsolationLevel = IsolationLevel.READ_COMMITTED): KafkaConsumerConfig {
        this[ConsumerConfig.ISOLATION_LEVEL_CONFIG] = level.value
        return this
    }

    fun <K : Any, V : Any> applyConfig(
        servers: String,
        groupId: String,
        clientId: String,
        keyDeserializer: Deserializer<K>,
        valueDeserializer: Deserializer<V>
    ): KafkaConsumerConfig {
        this[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = servers
        this[ConsumerConfig.GROUP_ID_CONFIG] = groupId
        this[ConsumerConfig.CLIENT_ID_CONFIG] = clientId
        this[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = keyDeserializer::class.java.name
        this[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = valueDeserializer::class.java.name

        return this
    }
}
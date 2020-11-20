/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties

interface IConsumerFactory {

    object DefaultConsumerFactory: IConsumerFactory {
        override fun <K, V> make(props: Properties): Consumer<K, V> = KafkaConsumer<K, V>(props)
    }

    fun <K, V> make(props: Properties): Consumer<K, V>
}
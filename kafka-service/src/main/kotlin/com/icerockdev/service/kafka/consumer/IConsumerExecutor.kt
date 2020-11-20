/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

interface IConsumerExecutor<K, V>: AutoCloseable {
    fun start(topics: List<String>, threadCount: Int = 1): Boolean

    fun pause(): Boolean

    fun resume(): Boolean
}

/*
 * Copyright 2020 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */

package com.icerockdev.service.kafka.consumer

interface IConsumerTask : AutoCloseable {
    suspend fun execute()

    fun pause()

    fun resume()
}
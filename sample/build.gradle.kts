/*
 * Copyright 2019 IceRock MAG Inc. Use of this source code is governed by the Apache 2.0 license.
 */
buildscript {
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:${properties["kotlin_version"]}")
    }
}

plugins {
    application
    id("org.jetbrains.kotlin.jvm")
    id("kotlin-kapt")
    id("idea")
}

group = "com.icerockdev"
version = "0.0.1"

apply(plugin = "kotlin")

repositories {
    maven { setUrl("https://dl.bintray.com/icerockdev/backend") }
}

application {
    mainClassName = "com.icerockdev.sample.Main"
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:${properties["kotlin_version"]}")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:${properties["coroutines_version"]}")

//    implementation("com.icerockdev.service:kafka-service:0.1.0")
    implementation(project(":kafka-service"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = JavaVersion.VERSION_11.toString()
    }
}
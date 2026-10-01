import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)

    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

group = "ru.quipy"
version = "0.0.1-SNAPSHOT"

description = "Application for resilience and highly-loaded applications course"

repositories {
    mavenCentral()
}

dependencies {
    // Spring
    implementation(libs.spring.web) {
        exclude(
            group = "org.springframework.boot",
            module = "spring-boot-starter-tomcat"
        )
    }

    implementation(libs.spring.jetty)
    implementation(libs.spring.actuator)

    // Kotlin
    implementation(libs.kotlin.reflect)
    implementation(libs.kotlin.coroutines.core)
    implementation(libs.kotlin.coroutines.reactor)

    // HTTP
    implementation(libs.okhttp)

    // Jetty / HTTP2
    implementation(libs.jetty.http2.server)

    // Jackson
    implementation(libs.jackson.kotlin)
    implementation(libs.jackson.java.time)

    // Cache
    implementation(libs.caffeine)

    // Resilience
    implementation(libs.resilience4j.ratelimiter)

    // Event Sourcing
    implementation(libs.tiny.es.spring)
    implementation(libs.tiny.es.postgres)

    // Metrics
    implementation(libs.micrometer.prometheus)

    // Tests
    testImplementation(libs.spring.test)
    testImplementation(libs.kotlin.test)
}

kotlin {
    jvmToolchain(17)

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)

        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}
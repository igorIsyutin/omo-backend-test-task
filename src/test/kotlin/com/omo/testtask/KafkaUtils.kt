package com.omo.testtask

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serdes
import org.testcontainers.containers.KafkaContainer
import java.util.Properties

fun Any.toJson(): String = jacksonObjectMapper().writeValueAsString(this)

fun KafkaContainer.createAdminClient(): AdminClient {
    val adminProps = Properties()
    adminProps[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    return AdminClient.create(adminProps)
}

fun KafkaContainer.createProducer(): KafkaProducer<String, String> {
    val producerProps = Properties()
    producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = Serdes.String().serializer().javaClass
    producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = Serdes.String().serializer().javaClass
    return KafkaProducer<String, String>(producerProps)
}

fun KafkaContainer.createConsumer(consumerGroupId: String): KafkaConsumer<String, String> {
    val consumerProps = Properties()
    consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
    consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer().javaClass
    consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer().javaClass
    consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
    consumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
    consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
    return KafkaConsumer<String, String>(consumerProps)
}
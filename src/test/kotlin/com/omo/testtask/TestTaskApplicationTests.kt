package com.omo.testtask

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders
import org.springframework.test.web.servlet.result.MockMvcResultMatchers
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.Properties

//TODO add your controller here
@WebMvcTest(controllers = [])
@Testcontainers
class TestTaskApplicationTests {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Container
    private val kafkaContainer = KafkaContainer()

    private val consumerGroupId = "test-group"
    private val topicName = "testTopic"

    @BeforeEach
    fun initConsumerGroup() {
        System.setProperty("kafka.bootstrap.servers", kafkaContainer.bootstrapServers)

        val adminClient = createAdminClient()
        //Init test topic
        adminClient.createTopics(mutableListOf(NewTopic(topicName, 3, 1))).all().get()

        val producer = createProducer()
        //Send 5 record in 1 partition
        repeat(5) {
            producer.send(ProducerRecord(topicName, 1, "ket", "value"))
        }

        //await consumer group to get partitions from kafka and poll one record
        val consumer = createConsumer()
        consumer.subscribe(listOf(topicName))
        while (consumer.poll(Duration.ZERO).isEmpty) {
        }
        consumer.commitSync()
    }

    @Test
    fun contextLoads() {
        mockMvc.perform(
            MockMvcRequestBuilders
                .get("/consumer-groups/lag/$consumerGroupId")
        ).andExpect(MockMvcResultMatchers.status().`is`(200))
            .andExpect(MockMvcResultMatchers.content().json(
                mapOf(
                    "testTopic-0" to 0,
                    "testTopic-1" to 4,
                    "testTopic-2" to 0,
                ).toJson()
            ))
    }

    private fun createAdminClient(): AdminClient {
        val adminProps = Properties()
        adminProps[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        return AdminClient.create(adminProps)
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val producerProps = Properties()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = Serdes.String().serializer().javaClass
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = Serdes.String().serializer().javaClass
        return KafkaProducer<String, String>(producerProps)
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaContainer.bootstrapServers
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer().javaClass
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = Serdes.String().deserializer().javaClass
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = consumerGroupId
        consumerProps[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return KafkaConsumer<String, String>(consumerProps)
    }

    companion object {
        private val mapper = jacksonObjectMapper()

        fun Any.toJson(): String = mapper.writeValueAsString(this)
    }
}

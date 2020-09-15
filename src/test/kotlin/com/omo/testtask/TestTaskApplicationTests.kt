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

        val adminClient = kafkaContainer.createAdminClient()
        //Init test topic
        adminClient.createTopics(mutableListOf(NewTopic(topicName, 3, 1))).all().get()

        val producer = kafkaContainer.createProducer()
        //Send 5 record in 1 partition
        repeat(5) {
            producer.send(ProducerRecord(topicName, 1, "key", "value"))
        }

        //await consumer group to get partitions from kafka and poll one record
        val consumer = kafkaContainer.createConsumer(consumerGroupId)
        consumer.subscribe(listOf(topicName))
        while (consumer.poll(Duration.ZERO).isEmpty) {
        }
        consumer.commitSync()
    }

    @Test
    fun getConsumerGroupLag() {
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
}

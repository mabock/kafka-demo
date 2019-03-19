package de.bockcoding.examples.kafkademo

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*
import javax.annotation.PostConstruct

@SpringBootApplication
class KafkaDemoApplication

fun main(args: Array<String>) {
    runApplication<KafkaDemoApplication>(*args)

}

@PostConstruct
fun postConstruct(){

}

@Component
class Producer(
        private val kafkaTemplate: KafkaTemplate<String, String>,
        private val objectMapper: ObjectMapper
){
    val TOPIC = "click-topic"
    fun sendNewClicksToKafkaTopic(
            counts: Int
    ){
        val clicks = Clicks(UUID.randomUUID(), LocalDateTime.now(), counts)

        kafkaTemplate.send(TOPIC, objectMapper.writeValueAsString(clicks))
    }
}

@RestController
class Controller(
        val producer: Producer
){
    val random: Random = Random()

    @PostMapping("clicks")
    fun postClicks(){
        producer.sendNewClicksToKafkaTopic(random.nextInt(5000))
    }
}


@Component
class Consumer
{
    @KafkaListener(topics = ["click-topic"], groupId = "group_id2")
    fun consume(message: String){
        println(message)
    }
}
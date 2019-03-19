package de.bockcoding.examples.kafkademo

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import org.springframework.web.bind.annotation.GetMapping
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
        val producer: Producer,
        private val eventStore: EventStore
){
    val random: Random = Random()

    @PostMapping("clicks")
    fun postClicks(){
        producer.sendNewClicksToKafkaTopic(random.nextInt(5000))
    }


    @GetMapping("clicks")
    fun getClicks(): MutableList<Clicks> {
        return eventStore.events
    }
}

@Component
class EventStore() {
    val events:MutableList<Clicks> = mutableListOf()
}



@Component
class Consumer (
        val objectMapper: ObjectMapper,
        private val eventStore: EventStore
)
{
    @KafkaListener(topics = ["click-topic"])
    fun consume(message: String){
        val clicks = objectMapper.readValue<Clicks>(message)
        println("counts: "+ clicks.count + "; uuid: "+clicks.eventId)
        eventStore.events.add(clicks)
    }
}
package de.bockcoding.examples.kafkademo

import java.time.LocalDateTime
import java.util.*

data class Clicks(
        val eventId: UUID,
        val time: LocalDateTime,
        val count: Int
)
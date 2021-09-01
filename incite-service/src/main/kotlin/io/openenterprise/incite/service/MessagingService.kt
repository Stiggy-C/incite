package io.openenterprise.incite.service

interface MessagingService {

    fun produce(topic: String, message: String, ordered: Boolean, replicated: Boolean)
}
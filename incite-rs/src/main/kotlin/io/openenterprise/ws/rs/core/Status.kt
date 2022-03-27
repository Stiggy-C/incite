package io.openenterprise.ws.rs.core

import javax.ws.rs.core.Response

enum class Status(private val statusCode: Int, private val reasonPhrase: String): Response.StatusType {

    PROCESSING(102, "Processing") {

        override fun getFamily(): Response.Status.Family = Response.Status.Family.familyOf(statusCode)
    };

    override fun getStatusCode(): Int = this.statusCode

    override fun getReasonPhrase(): String = this.reasonPhrase
}
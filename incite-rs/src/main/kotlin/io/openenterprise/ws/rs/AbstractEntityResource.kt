package io.openenterprise.ws.rs

import io.openenterprise.data.domain.AbstractEntity
import java.io.Serializable
import javax.ws.rs.container.AsyncResponse

interface AbstractEntityResource<T: AbstractEntity<ID>, ID: Serializable> {

    fun create(entity: T, asyncResponse: AsyncResponse)

    fun retrieve(id: ID, asyncResponse: AsyncResponse)

    fun delete(id: ID, asyncResponse: AsyncResponse)
}
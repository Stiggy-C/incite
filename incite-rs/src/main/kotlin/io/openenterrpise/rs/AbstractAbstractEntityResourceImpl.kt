package io.openenterrpise.rs

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.service.AbstractEntityService
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.Serializable
import javax.inject.Inject
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.Response

abstract class AbstractAbstractEntityResourceImpl<T: AbstractEntity<ID>, ID: Serializable> : AbstractEntityResource<T, ID> {

    @Inject
    lateinit var abstractEntityService: AbstractEntityService<T, ID>

    override fun create(entity: T, asyncResponse: AsyncResponse) {
        GlobalScope.launch {
            try {
                abstractEntityService.create(entity)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.CREATED).entity(entity).build())
        }
    }

    override fun retrieve(id: ID, asyncResponse: AsyncResponse) {
        GlobalScope.launch {
            var entity: T? = null
            try {
                entity = abstractEntityService.retrieve(id)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.ok().entity(entity).build())
        }
    }

    override fun delete(id: ID, asyncResponse: AsyncResponse) {
        GlobalScope.launch {
            try {
                abstractEntityService.delete(id)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.NO_CONTENT).build())
        }
    }
}
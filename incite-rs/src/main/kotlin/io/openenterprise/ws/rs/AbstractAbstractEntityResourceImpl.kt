package io.openenterprise.ws.rs

import io.openenterprise.data.domain.AbstractEntity
import io.openenterprise.service.AbstractEntityService
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import java.io.Serializable
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.Response

abstract class AbstractAbstractEntityResourceImpl<T: AbstractEntity<ID>, ID: Serializable> :
    AbstractEntityResource<T, ID> {

    @Autowired
    protected lateinit var abstractEntityService: AbstractEntityService<T, ID>

    @Autowired
    protected lateinit var coroutineScope: CoroutineScope

    override fun create(entity: T, asyncResponse: AsyncResponse) {
        coroutineScope.launch {
            try {
                abstractEntityService.create(entity)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.CREATED).entity(entity).build())
        }
    }

    override fun retrieve(id: ID, asyncResponse: AsyncResponse) {
        coroutineScope.launch {
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
        coroutineScope.launch {
            try {
                abstractEntityService.delete(id)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.NO_CONTENT).build())
        }
    }

    protected fun getAuthentication(): Authentication? {
        return SecurityContextHolder.getContext()?.authentication
    }
}
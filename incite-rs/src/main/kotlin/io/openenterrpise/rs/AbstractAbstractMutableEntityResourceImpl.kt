package io.openenterrpise.rs

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.data.domain.AbstractMutableEntity
import io.openenterprise.service.AbstractMutableEntityService
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.io.Serializable
import javax.inject.Inject
import javax.persistence.EntityNotFoundException
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.Response

class AbstractAbstractMutableEntityResourceImpl<T : AbstractMutableEntity<ID>, ID : Serializable> :
    AbstractAbstractEntityResourceImpl<T, ID>(), AbstractMutableEntityResource<T, ID> {

    @Inject
    lateinit var abstractMutableEntityService: AbstractMutableEntityService<T, ID>

    @Inject
    lateinit var objectMapper: ObjectMapper

    override fun update(id: ID, entity: T, asyncResponse: AsyncResponse) {
        GlobalScope.launch {
            try {
                val persistedEntity = abstractEntityService.retrieve(id)

                if (persistedEntity == null) {
                    asyncResponse.resume(EntityNotFoundException())
                }

                // TODO merge entity into persistedEntity

                abstractMutableEntityService.update(persistedEntity!!)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.CREATED).entity(entity).build())
        }
    }
}
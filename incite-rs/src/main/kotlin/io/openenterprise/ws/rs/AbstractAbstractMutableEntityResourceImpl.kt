package io.openenterprise.ws.rs

import com.fasterxml.jackson.databind.ObjectMapper
import io.openenterprise.data.domain.AbstractMutableEntity
import io.openenterprise.service.AbstractMutableEntityService
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.springframework.beans.factory.annotation.Autowired
import java.io.Serializable
import javax.persistence.EntityNotFoundException
import javax.ws.rs.container.AsyncResponse
import javax.ws.rs.core.Response

abstract class AbstractAbstractMutableEntityResourceImpl<T : AbstractMutableEntity<ID>, ID : Serializable> :
    AbstractAbstractEntityResourceImpl<T, ID>(), AbstractMutableEntityResource<T, ID> {

    @Autowired
    lateinit var abstractMutableEntityService: AbstractMutableEntityService<T, ID>

    @Autowired
    lateinit var objectMapper: ObjectMapper

    override fun update(id: ID, entity: T, asyncResponse: AsyncResponse) {
        coroutineScope.launch(Dispatchers.IO) {
            try {
                val persistedEntity = abstractEntityService.retrieve(id)

                if (persistedEntity == null) {
                    asyncResponse.resume(EntityNotFoundException())
                }

                val jsonString = objectMapper.writeValueAsString(entity)
                val updatedEntity = objectMapper.readerForUpdating(persistedEntity).readValue<T>(jsonString)

                abstractMutableEntityService.update(updatedEntity)
            } catch (e: Exception) {
                asyncResponse.resume(e)
            }

            asyncResponse.resume(Response.status(Response.Status.NO_CONTENT).build())
        }
    }
}
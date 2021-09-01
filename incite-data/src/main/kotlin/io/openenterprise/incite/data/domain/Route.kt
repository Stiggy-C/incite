package io.openenterprise.incite.data.domain

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.openenterprise.data.domain.AbstractMutableEntity
import java.util.*
import javax.persistence.*

@MappedSuperclass
@DiscriminatorColumn(name = "type")
@Inheritance(strategy = InheritanceType.SINGLE_TABLE)
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    value = [
        JsonSubTypes.Type(value = SpringXmlRoute::class, name = "SpringXML"),
        JsonSubTypes.Type(value = YamlRoute::class, name = "YAML")
    ]
)
abstract class Route : AbstractMutableEntity<UUID>() {

    @Version
    var version: Long? = null

    @PrePersist
    override fun prePersist() {
        super.prePersist()

        id = UUID.randomUUID()
    }
}
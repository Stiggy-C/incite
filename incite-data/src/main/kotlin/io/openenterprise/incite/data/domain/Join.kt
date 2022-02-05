package io.openenterprise.incite.data.domain

class Join {

    lateinit var leftColumn: String

    lateinit var rightColumn: String

    var rightIndex: Int = 0

    lateinit var type: Type

    enum class Type {

        CROSS, FULL, INNER, LEFT, RIGHT
    }
}
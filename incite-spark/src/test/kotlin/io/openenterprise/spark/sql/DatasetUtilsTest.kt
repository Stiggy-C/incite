package io.openenterprise.spark.sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import org.junit.Assert
import org.junit.Test
import scala.collection.JavaConversions
import java.util.*
import kotlin.test.assertNotNull

class DatasetUtilsTest {

    @Test
    fun toJson() {
        val list = arrayListOf<TestObject>()
        val random = Random()
        val sparkSession =
            SparkSession.builder().appName(DatasetUtilsTest::class.java.simpleName).master("local[1]").orCreate

        for (i in 0..1000000) {
            list.add(TestObject(UUID.randomUUID().toString(), random.nextLong()))
        }

        val dataset = sparkSession.createDataset(list, Encoders.bean(TestObject::class.java))
        val jsonString = DatasetUtils.toJson(dataset)
        Assert.assertNotNull(jsonString)

        val objectMapper = ObjectMapper()
        val jsonNode = objectMapper.readTree(jsonString)

        Assert.assertTrue(jsonNode is ArrayNode)
    }

    class TestObject() {

        constructor(id: String, field0: Long): this() {
            this.id = id
            this.field0 = field0
        }

        lateinit var id: String

        var field0: Long = 0L
    }
}
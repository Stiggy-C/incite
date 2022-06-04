package io.openenterprise.ignite.spark.impl

import io.openenterprise.ignite.spark.IgniteJdbcConstants
import org.apache.commons.lang3.BooleanUtils
import org.apache.commons.lang3.NotImplementedException
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.IgniteJdbcThinDriver
import org.apache.spark.SparkException
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.*
import org.slf4j.LoggerFactory
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.SingleConnectionDataSource
import scala.Function0
import scala.collection.immutable.Map
import java.io.Serializable
import java.sql.Connection
import java.util.*
import java.util.stream.Collectors

class IgniteRelationProvider : JdbcRelationProvider(), Serializable {

    companion object {

        @JvmStatic
        private val LOG = LoggerFactory.getLogger(IgniteRelationProvider::class.java)
    }

    override fun shortName(): String {
        return IgniteJdbcConstants.FORMAT
    }

    override fun createRelation(
        sqlContext: SQLContext,
        saveMode: SaveMode,
        parameters: Map<String, String>,
        dataset: Dataset<Row>
    ): BaseRelation {
        if (!parameters.contains(IgniteJdbcConstants.PRIMARY_KEY_COLUMNS)) {
            throw SparkException("${IgniteJdbcConstants.PRIMARY_KEY_COLUMNS} is not provided for ${IgniteJdbcConstants.FORMAT} format.")
        }

        if (!parameters.contains(JDBCOptions.JDBC_DRIVER_CLASS())) {
            parameters.updated(JDBCOptions.JDBC_DRIVER_CLASS(), IgniteJdbcThinDriver::class.java.name)
        }

        val jdbcOptionsInWrite = JdbcOptionsInWrite(parameters)
        val connection: Connection = JdbcUtils.createConnectionFactory(jdbcOptionsInWrite).apply()
        val singleConnectionDataSource = SingleConnectionDataSource(connection, false)
        val jdbcTemplate = JdbcTemplate(singleConnectionDataSource)

        val tableExists = tableExists(jdbcTemplate, parameters)

        if (tableExists) {
            when (saveMode) {
                SaveMode.ErrorIfExists -> {
                    throw SparkException("Table or view, ${jdbcOptionsInWrite.table()}, already exists. SaveMode: ErrorIfExists")
                }
                SaveMode.Ignore -> {
                    // Do nothing.
                }
                else -> {
                    when (saveMode) {
                        SaveMode.Append -> {
                            // Do nothing.
                        }
                        SaveMode.Overwrite -> {
                            if (jdbcOptionsInWrite.isTruncate) {
                                truncateTable(jdbcTemplate, parameters)
                            } else {
                                dropTable(jdbcTemplate, parameters)
                                createTable(jdbcTemplate, dataset, parameters)
                            }
                        }
                        else ->
                            throw SparkException("Should not happen", IllegalStateException())
                    }

                    saveTable(dataset, parameters)
                }
            }
        } else {
            createTable(jdbcTemplate, dataset, parameters)
            saveTable(dataset, parameters)
        }

        connection.close()

        return this.createRelation(sqlContext, parameters)
    }

    private class ForEachPartitionSaveTableFunction(
        private val connectionFactory: Function0<Connection>,
        private val insertStatement: String,
        private val schema: StructType
    ) : ForeachPartitionFunction<Row>, Serializable {

        override fun call(iterator: MutableIterator<Row>) {
            val valuesList = mutableListOf<Array<Any>>()
            val fields = schema.fields()

            iterator.forEachRemaining { row ->
                val values = Arrays.stream(fields)
                    .map { field ->
                        row.get(row.fieldIndex(field.name()))
                    }
                    .toArray()

                valuesList.add(values)
            }

            val connection: Connection = connectionFactory.apply()
            val singleConnectionDataSource = SingleConnectionDataSource(connection, false)
            val jdbcTemplate = JdbcTemplate(singleConnectionDataSource)

            LoggerFactory.getLogger(this::class.java)
                .info("About to save partition with statement, $insertStatement")

            jdbcTemplate.batchUpdate(insertStatement, valuesList)

            connection.close()
        }
    }

    private fun createTable(jdbcTemplate: JdbcTemplate, dataset: Dataset<Row>, parameters: Map<String, String>) {
        val jdbcOptionsInWrite = JdbcOptionsInWrite(parameters)

        val caseSensitive = parameters.contains(IgniteJdbcConstants.CASE_SENSITIVE) &&
                BooleanUtils.toBoolean(parameters.get(IgniteJdbcConstants.CASE_SENSITIVE).get())
        val columnOverrides: MutableMap<String, String> = getColumnsOverrides(jdbcOptionsInWrite)
        val datasetFields = dataset.schema().fields()
        val tableName = getTableName(parameters)

        val createTableStatement = buildString {
            append("CREATE TABLE $tableName (")

            val tableColumns = Arrays.stream(datasetFields).map {
                val columnName: String = if (caseSensitive) "\"${it.name()}\"" else it.name()
                val dataType: String = if (columnOverrides.containsKey(it.name())) {
                    columnOverrides[it.name()]!!
                } else {
                    getIgniteDataType(it.dataType()).databaseTypeDefinition()
                }

                "$columnName $dataType"
            }.collect(Collectors.joining(", "))

            append(tableColumns)

            val primaryKeyColumns =
                Arrays.stream(
                    StringUtils.split(parameters[IgniteJdbcConstants.PRIMARY_KEY_COLUMNS].get(), ",")
                )
                    .map {
                        if (caseSensitive) "\"$it\"" else it
                    }
                    .collect(Collectors.joining(", "))

            append(", PRIMARY KEY ($primaryKeyColumns)")

            append(")")

            if (StringUtils.isNotEmpty(jdbcOptionsInWrite.createTableOptions())) {
                append(" WITH ${jdbcOptionsInWrite.createTableOptions()}")
            }
        }

        LOG.info("About to create table, $tableName, with statement, $createTableStatement")

        jdbcTemplate.update(createTableStatement)
    }

    private fun dropTable(jdbcTemplate: JdbcTemplate, parameters: Map<String, String>) {
        val tableName = getTableName(parameters)

        val dropTableStatement = "DROP TABLE IF EXISTS $tableName"

        LOG.info("About to drop table, $tableName")

        jdbcTemplate.update(dropTableStatement)
    }

    private fun getColumnsOverrides(jdbcOptionsInWrite: JdbcOptionsInWrite): MutableMap<String, String> {
        val columnOverrides: MutableMap<String, String> = if (jdbcOptionsInWrite.createTableColumnTypes().isEmpty) {
            Collections.emptyMap()
        } else {
            val createTableColumnTypes = jdbcOptionsInWrite.createTableColumnTypes().get()
            val overrideColumns = StringUtils.split(createTableColumnTypes, ",", 2)

            Arrays.stream(overrideColumns)
                .map {
                    val tokens = it.split(" ")

                    Pair(tokens[0], tokens[1])
                }
                .collect(Collectors.toMap(Pair<String, String>::first, Pair<String, String>::second))
        }
        return columnOverrides
    }

    private fun getIgniteDataType(dataType: DataType): JdbcType {
        return when (dataType) {
            is BooleanType ->
                JdbcType("BOOLEAN", -7)
            is BinaryType ->
                JdbcType("BINARY", 2004)
            is ByteType ->
                JdbcType("TINYINT", -6)
            is DateType ->
                JdbcType("DATE", 91)
            is DecimalType ->
                JdbcType("DECIMAL",  3)
            is DoubleType ->
                JdbcType("DOUBLE", 8)
            is FloatType ->
                JdbcType("REAL", 6)
            is IntegerType ->
                JdbcType("INTEGER",4)
            is LongType ->
                JdbcType("BIGINT", -5)
            is ShortType ->
                JdbcType("SMALLINT", 5)
            is StringType ->
                JdbcType("VARCHAR", 2005)
            is TimestampType ->
                JdbcType("TIMESTAMP", 93)
            else ->
                throw SparkException("Given type not supported by Apache Ignite", UnsupportedOperationException())
        }
    }

    private fun getTableName(parameters: Map<String, String>): String? {
        val caseSensitive = parameters.contains(IgniteJdbcConstants.CASE_SENSITIVE) &&
                BooleanUtils.toBoolean(parameters.get(IgniteJdbcConstants.CASE_SENSITIVE).get())
        val jdbcOptionsInWrite = JdbcOptionsInWrite(parameters)

        val tableName = if (caseSensitive) {
            "\"${jdbcOptionsInWrite.table()}\""
        } else {
            jdbcOptionsInWrite.table()
        }

        return tableName
    }

    private fun saveTable(dataset: Dataset<Row>, parameters: Map<String, String>) {
        val caseSensitive = parameters.contains(IgniteJdbcConstants.CASE_SENSITIVE) &&
                BooleanUtils.toBoolean(parameters.get(IgniteJdbcConstants.CASE_SENSITIVE).get())
        val schema = dataset.schema()
        val fields = schema.fields()
        val tableName = getTableName(parameters)

        val mergeStatement = buildString {
            append("MERGE INTO $tableName (")

            val columns = Arrays.stream(fields).map {
                if (caseSensitive) "\"${it.name()}\"" else it.name()
            }.collect(Collectors.joining(", "))

            append(columns)
            append(") values (")

            val params = Arrays.stream(fields).map {
                "?"
            }.collect(Collectors.joining(", "))

            append(params)
            append(")")
        }

        val jdbcOptionsInWrite = JdbcOptionsInWrite(parameters)
        val createConnectionFactoryFunction = JdbcUtils.createConnectionFactory(jdbcOptionsInWrite)

        dataset.foreachPartition(
            ForEachPartitionSaveTableFunction(createConnectionFactoryFunction, mergeStatement, schema)
        )
    }

    private fun tableExists(jdbcTemplate: JdbcTemplate, parameters: Map<String, String>): Boolean {
        val tableName = getTableName(parameters)
        val selectStatement = "SELECT 1 FROM $tableName WHERE 1=0"

        try {
            jdbcTemplate.execute(selectStatement)
        } catch (e: Exception) {
            return false
        }

        return true
    }

    private fun truncateTable(jdbcTemplate: JdbcTemplate, parameters: Map<String, String>) {
        throw NotImplementedException()
    }
}
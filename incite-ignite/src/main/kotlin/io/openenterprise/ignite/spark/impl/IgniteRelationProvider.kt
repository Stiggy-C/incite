package io.openenterprise.ignite.spark.impl

import io.openenterprise.ignite.spark.IgniteContext
import io.openenterprise.ignite.spark.IgniteDataFrameConstants
import io.openenterprise.springframework.context.ApplicationContextUtils
import org.apache.commons.lang3.StringUtils
import org.apache.ignite.spark.IgniteDataFrameSettings
import org.apache.ignite.spark.impl.IgniteSQLRelation
import org.apache.ignite.spark.impl.QueryHelper
import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import scala.Option
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable.Map
import javax.inject.Inject

/**
 * As of 2022-01-18, Spark optimization is not being handled.
 */
class IgniteRelationProvider : org.apache.ignite.spark.impl.IgniteRelationProvider() {

    @Inject
    lateinit var igniteContext: IgniteContext

    init {
        assert(ApplicationContextUtils.getApplicationContext() != null)

        ApplicationContextUtils.getApplicationContext()!!.autowireCapableBeanFactory.autowireBean(this)
    }

    override fun createRelation(sqlContext: SQLContext, parameters: Map<String, String>): BaseRelation {
        return IgniteSQLRelation<Any, Any>(
            igniteContext,
            parameters[IgniteDataFrameSettings.OPTION_TABLE()].get(),
            parameters[IgniteDataFrameSettings.OPTION_SCHEMA()],
            sqlContext
        )
    }

    override fun createRelation(
        sqlContext: SQLContext,
        saveMode: SaveMode,
        parameters: Map<String, String>,
        dataset: Dataset<Row>
    ): BaseRelation {
        val ignite = igniteContext.ignite()
        val igniteSchema = parameters[IgniteDataFrameSettings.OPTION_SCHEMA()]
        val schema = dataset.schema()
        val tableName = parameters[IgniteDataFrameSettings.OPTION_TABLE()].get()
        val tableExists = igniteContext.ignite().cacheNames().contains(tableName)
        val tableConfiguration = if (tableExists) igniteContext.ignite().cache<Any, Any>(tableName) else null

        if (tableConfiguration == null) {
            QueryHelper.ensureCreateTableOptions(schema, parameters, igniteContext)

            val createTableParameters = parameters[IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS()]
            val primaryKeys = StringUtils.split(
                parameters[IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS()].get(),
                ','
            ).asList()

            QueryHelper.createTable(
                schema,
                tableName,
                asScalaBufferConverter(primaryKeys).asScala().toSeq(),
                createTableParameters,
                ignite
            )
            QueryHelper.saveTable(
                dataset, tableName, igniteSchema, igniteContext,
                parameters[IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE()] as Option<Any>,
                parameters[IgniteDataFrameSettings.OPTION_STREAMER_SKIP_STORE()] as Option<Any>,
                parameters[IgniteDataFrameSettings.OPTION_STREAMER_FLUSH_FREQUENCY()] as Option<Any>,
                parameters[IgniteDataFrameSettings.OPTION_STREAMER_PER_NODE_BUFFER_SIZE()] as Option<Any>,
                parameters[IgniteDataFrameSettings.OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS()] as Option<Any>
            )
        } else {
            when (saveMode) {
                SaveMode.ErrorIfExists -> {
                    throw SparkException("Table or view, $tableName, already exists. SaveMode: ErrorIfExists.")
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
                            val createTableParameters =
                                parameters[IgniteDataFrameSettings.OPTION_CREATE_TABLE_PARAMETERS()]
                            val primaryKeys = StringUtils.split(
                                parameters[IgniteDataFrameSettings.OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS()].get(),
                                ','
                            ).asList()

                            QueryHelper.ensureCreateTableOptions(schema, parameters, igniteContext)
                            QueryHelper.dropTable(tableName, ignite)
                            QueryHelper.createTable(
                                schema,
                                tableName,
                                asScalaBufferConverter(primaryKeys).asScala().toSeq(),
                                createTableParameters,
                                ignite
                            )
                        }
                        else -> throw IllegalStateException()
                    }

                    QueryHelper.saveTable(
                        dataset, tableName, igniteSchema, igniteContext,
                        parameters[IgniteDataFrameSettings.OPTION_STREAMER_ALLOW_OVERWRITE()] as Option<Any>,
                        parameters[IgniteDataFrameSettings.OPTION_STREAMER_SKIP_STORE()] as Option<Any>,
                        parameters[IgniteDataFrameSettings.OPTION_STREAMER_FLUSH_FREQUENCY()] as Option<Any>,
                        parameters[IgniteDataFrameSettings.OPTION_STREAMER_PER_NODE_BUFFER_SIZE()] as Option<Any>,
                        parameters[IgniteDataFrameSettings.OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS()] as Option<Any>
                    )
                }
            }
        }

        return this.createRelation(sqlContext, parameters)
    }

    override fun shortName(): String {
        return IgniteDataFrameConstants.FORMAT
    }

}
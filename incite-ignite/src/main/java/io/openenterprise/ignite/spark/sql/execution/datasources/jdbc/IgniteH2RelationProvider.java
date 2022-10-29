package io.openenterprise.ignite.spark.sql.execution.datasources.jdbc;

import com.google.common.collect.Lists;
import io.openenterprise.ignite.spark.IgniteJdbcConstants;
import io.openenterprise.ignite.spark.sql.jdbc.IgniteDialect;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.shaded.org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcOptionsInWrite;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider;
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import scala.Function1;
import scala.collection.Iterator;
import scala.collection.immutable.Map;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IgniteH2RelationProvider extends JdbcRelationProvider implements Serializable {

    @Override
    public String shortName() {
        return IgniteJdbcConstants.getIGNITE_FORMAT();
    }

    @Override
    public BaseRelation createRelation(SQLContext sqlContext, SaveMode saveMode, Map<String, String> parameters, Dataset<Row> dataset) {
        if (!parameters.contains(IgniteJdbcConstants.getPRIMARY_KEY_COLUMNS())) {
            throw new RuntimeException(new SparkException(IgniteJdbcConstants.getPRIMARY_KEY_COLUMNS() +
                    " is not provided for " + IgniteJdbcConstants.getIGNITE_FORMAT() + " format"));
        }

        if (!parameters.contains(JDBCOptions.JDBC_DRIVER_CLASS())) {
            parameters.updated(JDBCOptions.JDBC_DRIVER_CLASS(), IgniteJdbcThinDriver.class);
        }

        IgniteDialect igniteDialect = new IgniteDialect();
        JdbcOptionsInWrite jdbcOptionsInWrite = new JdbcOptionsInWrite(parameters);
        String table = jdbcOptionsInWrite.table();


        boolean tableExists = isTableExists(igniteDialect, jdbcOptionsInWrite, table);

        if (tableExists) {
            switch (saveMode) {
                case ErrorIfExists:
                    throw new RuntimeException("Table or view, " + table + ", already exists. SaveMode: ErrorIfExists");
                case Ignore:
                    break;
                default:
                    if (saveMode == SaveMode.Overwrite) {
                        if (jdbcOptionsInWrite.isTruncate()) {
                            throw new NotImplementedException();
                        } else {
                            dropTable(jdbcOptionsInWrite, table);
                            createTable(jdbcOptionsInWrite, dataset.schema());
                        }
                    }

                    saveTable(jdbcOptionsInWrite, dataset);
            }
        } else {
            createTable(jdbcOptionsInWrite, dataset.schema());
            saveTable(jdbcOptionsInWrite, dataset);
        }

        return this.createRelation(sqlContext, parameters);
    }

    public /*ForeachPartitionFunction<Row>*/ Function1<Iterator<Row>, BoxedUnit> createSavePartitionFunction(
            JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String insertStatement) {
        return new SavePartitionFunction(jdbcOptionsInWrite, schema, insertStatement);
    }

    public /*ForeachFunction<Row>*/ Function1<Row, BoxedUnit> createSaveRowFunction(
            JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String insertStatement) {
        return new SaveRowFunction(jdbcOptionsInWrite, schema, insertStatement);
    }

    protected static Object[] mapRowToValues(StructField[] fields, Row row) {
        Object[] valuesArray = new Object[fields.length];

        for (int i = 0; i < fields.length; i++) {
            valuesArray[i] = row.get(i);
        }

        return valuesArray;
    }

    private void createTable(JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema) {
        boolean caseSensitive = isCaseSensitive(jdbcOptionsInWrite);
        java.util.Map<String, String> columnOverrides = getColumnsOverrides(jdbcOptionsInWrite);
        IgniteDialect igniteDialect = new IgniteDialect();
        String primaryKeyColumns = Arrays.stream(StringUtils.split(
                        jdbcOptionsInWrite.parameters().get(IgniteJdbcConstants.getPRIMARY_KEY_COLUMNS()).get(), ","))
                .map(it -> caseSensitive ? "\"" + it + "\"" : it)
                .collect(Collectors.joining(", "));
        StructField[] structFields = schema.fields();
        String table = jdbcOptionsInWrite.table();
        String tableColumns = Arrays.stream(structFields).map(it -> {
            String columnName = (caseSensitive) ? "\"" + it.name() + "\"" : it.name();
            String dataType = columnOverrides.containsKey(it.name()) ? columnOverrides.get(it.name()) :
                    igniteDialect.getJDBCType(it.dataType()).get().databaseTypeDefinition();

            return columnName + " " + dataType;
        }).collect(Collectors.joining(", "));

        String createTableStatement = "CREATE TABLE " + table + " (" +
                tableColumns + ", " +
                "PRIMARY KEY (" + primaryKeyColumns + ")" +
                ")";

        JdbcUtils.withConnection(jdbcOptionsInWrite,
                (Function1<Connection, Void>) connection -> {
                    getSingleUseJdbcTemplate(connection).execute(createTableStatement);
                    return null;
                });
    }

    private void dropTable(JdbcOptionsInWrite jdbcOptionsInWrite, String table) {
        JdbcUtils.withConnection(jdbcOptionsInWrite,
                (Function1<Connection, Void>) connection -> {
                    getSingleUseJdbcTemplate(connection).execute("DROP TABLE IF EXISTS " + table);
                    return null;
                });
    }

    private java.util.Map<String, String> getColumnsOverrides(JdbcOptionsInWrite jdbcOptionsInWrite) {
        if (jdbcOptionsInWrite.createTableColumnTypes().isEmpty()) {
            return Collections.emptyMap();
        } else {
            String createTableColumnTypes = jdbcOptionsInWrite.createTableColumnTypes().get();
            String[] overrideColumns = StringUtils.split(createTableColumnTypes, ",", 2);

            return Arrays.stream(overrideColumns)
                    .map(overrideColumn -> {
                        String[] tokens = StringUtils.split(overrideColumn, ' ');

                        return Pair.of(tokens[0], tokens[1]);
                    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        }
    }

    @NotNull
    private JdbcTemplate getSingleUseJdbcTemplate(Connection connection) {
        return new JdbcTemplate(new SingleConnectionDataSource(connection, false));
    }

    private boolean isCaseSensitive(JdbcOptionsInWrite jdbcOptionsInWrite) {
        return jdbcOptionsInWrite.parameters().get(IgniteJdbcConstants.getCASE_SENSITIVE()).nonEmpty() &&
                BooleanUtils.toBoolean(jdbcOptionsInWrite.parameters().get(IgniteJdbcConstants.getCASE_SENSITIVE()).get());
    }

    private boolean isTableExists(IgniteDialect igniteDialect, JdbcOptionsInWrite jdbcOptionsInWrite, String table) {
        return JdbcUtils.withConnection(jdbcOptionsInWrite,
                connection -> {
                    try {
                        return getSingleUseJdbcTemplate(connection).query(igniteDialect.getTableExistsQuery(table), rs -> true);
                    } catch (Exception e) {
                        Throwable rootCause = ExceptionUtils.getRootCause(e);

                        if (StringUtils.containsAnyIgnoreCase(
                                rootCause.getMessage(), "Table \"" + table + "\" not found")) {
                            return false;
                        }

                        throw e;
                    }
                });
    }

    private void saveTable(JdbcOptionsInWrite jdbcOptionsInWrite, Dataset<Row> dataset) {
        boolean caseSensitive = isCaseSensitive(jdbcOptionsInWrite);
        StructType schema = dataset.schema();
        StructField[] fields = schema.fields();

        String table = jdbcOptionsInWrite.table();
        String columns = Arrays.stream(fields)
                .map(it -> caseSensitive? "\"" + it.name() +"\"" : it.name())
                .collect(Collectors.joining(", "));
        String params = Arrays.stream(fields).map (it -> "?").collect(Collectors.joining(", "));

        String mergeStatement = "MERGE INTO " + table + "(" +
                columns +
                ") values (" +
                params +
                ")";

        //dataset.rdd().foreach(createSaveRowFunction(jdbcOptionsInWrite, schema, mergeStatement));

        //dataset.foreachPartition(createSavePartitionFunction(jdbcOptionsInWrite, schema, mergeStatement));

        dataset.rdd().foreachPartition(createSavePartitionFunction(jdbcOptionsInWrite, schema, mergeStatement));
    }

    protected class BatchUpdateFunction implements Function1<Connection, int[]>, Serializable {

        private String sqlStatement;

        private List<Object[]> valuesArrayList;

        public BatchUpdateFunction(String sqlStatement, List<Object[]> valuesArrayList) {
            this.sqlStatement = sqlStatement;
            this.valuesArrayList = valuesArrayList;
        }

        @Override
        public int[] apply(Connection connection) {
            return getSingleUseJdbcTemplate(connection).batchUpdate(sqlStatement, valuesArrayList);
        }

        public String getSqlStatement() {
            return sqlStatement;
        }

        public void setSqlStatement(String sqlStatement) {
            this.sqlStatement = sqlStatement;
        }

        public List<Object[]> getValuesArrayList() {
            return valuesArrayList;
        }

        public void setValuesArrayList(List<Object[]> valuesArrayList) {
            this.valuesArrayList = valuesArrayList;
        }
    }

    /*protected class SavePartitionFunction implements ForeachPartitionFunction<Row>, Serializable {

        private JdbcOptionsInWrite jdbcOptionsInWrite;

        private StructType schema;

        private String sqlStatement;

        public SavePartitionFunction(JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String sqlStatement) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
            this.schema = schema;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public void call(java.util.Iterator<Row> iterator) throws Exception {
            List<Object[]> valuesArrayList = Lists.newArrayList();
            StructField[] fields = schema.fields();

            while (iterator.hasNext()) {
                Row row = iterator.next();
                Object[] values = mapRowToValues(fields, row);

                valuesArrayList.add(values);
            }

            JdbcUtils.withConnection(jdbcOptionsInWrite, new BatchUpdateFunction(sqlStatement, valuesArrayList));
        }

        public JdbcOptionsInWrite getJdbcOptionsInWrite() {
            return jdbcOptionsInWrite;
        }

        public void setJdbcOptionsInWrite(JdbcOptionsInWrite jdbcOptionsInWrite) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
        }

        public StructType getSchema() {
            return schema;
        }

        public void setSchema(StructType schema) {
            this.schema = schema;
        }

        public String getSqlStatement() {
            return sqlStatement;
        }

        public void setSqlStatement(String sqlStatement) {
            this.sqlStatement = sqlStatement;
        }
    }*/

    protected class SavePartitionFunction implements Function1<Iterator<Row>, BoxedUnit>, Serializable {

        private JdbcOptionsInWrite jdbcOptionsInWrite;

        private StructType schema;

        private String sqlStatement;

        public SavePartitionFunction(JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String sqlStatement) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
            this.schema = schema;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public BoxedUnit apply(Iterator<Row> iterator) {
            List<Object[]> valuesArrayList = Lists.newArrayList();
            StructField[] fields = schema.fields();

            while (iterator.hasNext()) {
                Row row = iterator.next();
                Object[] values = mapRowToValues(fields, row);

                valuesArrayList.add(values);
            }

            JdbcUtils.withConnection(jdbcOptionsInWrite, new BatchUpdateFunction(sqlStatement, valuesArrayList));

            return BoxedUnit.UNIT;
        }

        public JdbcOptionsInWrite getJdbcOptionsInWrite() {
            return jdbcOptionsInWrite;
        }

        public void setJdbcOptionsInWrite(JdbcOptionsInWrite jdbcOptionsInWrite) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
        }

        public StructType getSchema() {
            return schema;
        }

        public void setSchema(StructType schema) {
            this.schema = schema;
        }

        public String getSqlStatement() {
            return sqlStatement;
        }

        public void setSqlStatement(String sqlStatement) {
            this.sqlStatement = sqlStatement;
        }
    }

    /*protected class SaveRowFunction implements ForeachFunction<Row>, Serializable {

        private JdbcOptionsInWrite jdbcOptionsInWrite;

        private StructType schema;

        private String sqlStatement;

        public SaveRowFunction(JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String sqlStatement) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
            this.schema = schema;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public void call(Row row) throws Exception {
            Object[] values = mapRowToValues(schema.fields(), row);

            JdbcUtils.withConnection(jdbcOptionsInWrite,
                    new BatchUpdateFunction(sqlStatement, Lists.<Object[]>newArrayList(values)));

        }
    }*/

    protected class SaveRowFunction implements Function1<Row, BoxedUnit>, Serializable {

        private JdbcOptionsInWrite jdbcOptionsInWrite;

        private StructType schema;

        private String sqlStatement;

        public SaveRowFunction(JdbcOptionsInWrite jdbcOptionsInWrite, StructType schema, String sqlStatement) {
            this.jdbcOptionsInWrite = jdbcOptionsInWrite;
            this.schema = schema;
            this.sqlStatement = sqlStatement;
        }

        @Override
        public BoxedUnit apply(Row row) {
            Object[] values = mapRowToValues(schema.fields(), row);

            JdbcUtils.withConnection(jdbcOptionsInWrite,
                    new BatchUpdateFunction(sqlStatement, Lists.<Object[]>newArrayList(values)));

            return BoxedUnit.UNIT;
        }
    }
}


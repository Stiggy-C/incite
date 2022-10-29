package io.openenterprise.ignite.spark.sql.jdbc;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.jdbc.*;
import org.apache.spark.sql.types.*;
import scala.Option;

public class IgniteDialect extends JdbcDialect {

    static {
        JdbcDialects.registerDialect(new IgniteDialect());
    }

    @Override
    public boolean canHandle(String url) {
        return StringUtils.startsWithIgnoreCase(url, "jdbc:ignite:thin");
    }

    @Override
    public Option<JdbcType> getJDBCType(DataType dataType) {
        JdbcType jdbcType;
        if (dataType instanceof BooleanType) {
            jdbcType = new JdbcType("BOOLEAN", -7);
        } else if (dataType instanceof BinaryType) {
            jdbcType = new JdbcType("BINARY", 2004);
        } else if (dataType instanceof ByteType) {
            jdbcType = new JdbcType("TINYINT", -6);
        } else if (dataType instanceof DateType) {
            jdbcType = new JdbcType("DATE", 91);
        } else if (dataType instanceof DecimalType) {
            jdbcType = new JdbcType("DECIMAL", 3);
        } else if (dataType instanceof  DoubleType) {
            jdbcType = new JdbcType("DOUBLE", 8);
        } else if (dataType instanceof FloatType) {
            jdbcType = new JdbcType("REAL", 6);
        } else if (dataType instanceof IntegerType) {
            jdbcType = new JdbcType("INTEGER", 4);
        } else if (dataType instanceof LongType) {
            jdbcType = new JdbcType("BIGINT", -5);
        } else if (dataType instanceof ShortType) {
            jdbcType = new JdbcType("SMALLINT", 5);
        } else if (dataType instanceof StringType) {
            jdbcType = new JdbcType("VARCHAR", 2005);
        } else if (dataType instanceof TimestampType) {
            jdbcType = new JdbcType("TIMESTAMP", 93);
        } else
            jdbcType = null;

        return Option.apply(jdbcType);
    }

    @Override
    public boolean isSupportedFunction(String funcName) {
        // TODO Handle Ignite specific functions
        return H2Dialect$.MODULE$.isSupportedFunction(funcName);
    }

    @Override
    public Option<String> compileAggregate(AggregateFunc aggFunction) {
        return H2Dialect$.MODULE$.compileAggregate(aggFunction);
    }

    @Override
    public AnalysisException classifyException(String message, Throwable e) {
        // TODO Handle Ignite specific exceptions
        return H2Dialect$.MODULE$.classifyException(message, e);
    }

}

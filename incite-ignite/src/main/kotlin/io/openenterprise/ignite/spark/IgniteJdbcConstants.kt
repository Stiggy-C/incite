package io.openenterprise.ignite.spark

sealed class IgniteJdbcConstants {

    companion object {

        @JvmStatic
        val CASE_SENSITIVE = "caseSensitive"

        @JvmStatic
        val FORMAT = "ignite"

        @JvmStatic
        val PRIMARY_KEY_COLUMNS = "primaryKeyFields"
    }
}
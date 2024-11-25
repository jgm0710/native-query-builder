package com.example.nativequerybuilder.generator


class NativeQueryBuilder {

    var query: String = ""

    val queryParams = mutableMapOf<String, Any?>()

    val batchQueryParams = mutableListOf<MutableMap<String, Any?>>()


    fun with(name: String, queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += "WITH $name AS ("
        this.queryBuilder()
        query += ") "
        return this
    }

    fun andWith(name: String, queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += ", $name AS ("
        this.queryBuilder()
        query += ") "
        return this
    }

    fun select(vararg columns: Any): NativeQueryBuilder {
        columns.forEach {
            require(it is String || it is Int) { "Column must be either String or Int" }
        }
        query += "SELECT ${columns.joinToString(", ")} "
        return this
    }

    fun from(table: String): NativeQueryBuilder {
        query += "FROM $table "
        return this
    }

    fun join(table: String): NativeQueryBuilder {
        query += "JOIN $table "
        return this
    }

    fun on(condition: String): NativeQueryBuilder {
        query += "ON $condition "
        return this
    }

    fun innerJoin(table: String, condition: String): NativeQueryBuilder {
        query += "INNER JOIN $table ON $condition "
        return this
    }

    fun leftJoin(table: String, condition: String): NativeQueryBuilder {
        query += "LEFT JOIN $table ON $condition "
        return this
    }

    fun rightJoin(table: String, condition: String): NativeQueryBuilder {
        query += "RIGHT JOIN $table ON $condition "
        return this
    }

    fun fullJoin(table: String, condition: String): NativeQueryBuilder {
        query += "FULL JOIN $table ON $condition "
        return this
    }

    fun crossJoin(table: String): NativeQueryBuilder {
        query += "CROSS JOIN $table "
        return this
    }

    fun where(condition: String): NativeQueryBuilder {
        query += "WHERE $condition "
        return this
    }

    fun where(queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += "WHERE "
        this.queryBuilder()
        return this
    }

    fun `if`(condition: Boolean, queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        if (condition) {
            this.queryBuilder()
        }
        return this
    }


    fun just(query: String): NativeQueryBuilder {
        this.query += "$query "
        return this
    }

    fun and(condition: String? = null): NativeQueryBuilder {
        query += "AND ${condition ?: ""} "
        return this
    }

    fun and(queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += "AND ("
        this.queryBuilder()
        query += ") "
        return this
    }

    fun or(condition: String? = null): NativeQueryBuilder {
        query += "OR ${condition ?: ""} "
        return this
    }

    fun or(queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += "OR ("
        this.queryBuilder()
        query += ") "
        return this
    }

    fun group(queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        query += "("
        this.queryBuilder()
        query += ") "
        return this
    }

    fun orderBy(vararg columns: String): NativeQueryBuilder {
        query += "ORDER BY ${columns.joinToString(", ")} "
        return this
    }

    fun limit(limit: Int): NativeQueryBuilder {
        query += "LIMIT $limit "
        return this
    }

    fun offset(offset: Int): NativeQueryBuilder {
        query += "OFFSET $offset "
        return this
    }

    fun addParam(key: String, value: Any?): NativeQueryBuilder {
        queryParams[key] = value
        return this
    }

    fun addParams(vararg  pairs : Pair<String, Any?>): NativeQueryBuilder {
        queryParams.putAll(pairs)
        return this
    }

    fun loop(loopCount: Int, queryBuilder: NativeQueryBuilder.() -> Unit): NativeQueryBuilder {
        for (i in 0 until loopCount) {
            this.queryBuilder()
        }
        return this
    }

    fun <T> mapLoop(collections : Collection<T>, queryBuilder: NativeQueryBuilder.(T) -> Unit): NativeQueryBuilder {
        collections.forEach {
            this.queryBuilder(it)
        }
        return this
    }

    fun addBatchParams(vararg  pairs : Pair<String, Any?>): NativeQueryBuilder {
        batchQueryParams.add(pairs.toMap().toMutableMap())
        return this
    }

    fun groupBy(vararg columns: String): NativeQueryBuilder {
        query += "GROUP BY ${columns.joinToString(", ")} "
        return this
    }

    fun having(condition: String): NativeQueryBuilder {
        query += "HAVING $condition "
        return this
    }

    fun insert(table: String): NativeQueryBuilder {
        query += "INSERT INTO $table "
        return this
    }

    fun columns(vararg columns: String): NativeQueryBuilder {
        query += "(${columns.joinToString(", ")}) "
        return this
    }

    fun values(vararg values: Any?): NativeQueryBuilder {
        query += "VALUES (${values.joinToString(", ")}) "
        return this
    }

    fun update(table: String): NativeQueryBuilder {
        query += "UPDATE $table "
        return this
    }

    fun set(column: String, value: Any?): NativeQueryBuilder {
        query += "SET $column = :value "
        queryParams["value"] = value
        return this
    }

    fun delete(table: String): NativeQueryBuilder {
        query += "DELETE FROM $table "
        return this
    }
}

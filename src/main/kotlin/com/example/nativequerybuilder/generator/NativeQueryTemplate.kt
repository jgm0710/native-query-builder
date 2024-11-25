package com.example.nativequerybuilder.generator

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import kotlin.reflect.KClass

@Component
class NativeQueryTemplate(
    private val namedParameterJdbcTemplate: NamedParameterJdbcTemplate,
    private val objectMapper: ObjectMapper,
) {

    fun queryForList(nativeQueryBuilder: NativeQueryBuilder): List<Map<String, Any?>> {
        return namedParameterJdbcTemplate
            .queryForList(nativeQueryBuilder.query, nativeQueryBuilder.queryParams)
    }

    fun queryForMap(nativeQueryBuilder: NativeQueryBuilder): Map<String, Any?>? {
        return namedParameterJdbcTemplate
            .queryForMap(nativeQueryBuilder.query, nativeQueryBuilder.queryParams)
    }

    fun <T : Any> queryForEntities(nativeQueryBuilder: NativeQueryBuilder, kClass: KClass<T>): List<T> {
        return this.queryForList(nativeQueryBuilder)
            .map { objectMapper.convertValue(it, kClass.java) }
    }

    fun <T : Any> queryForEntity(nativeQueryBuilder: NativeQueryBuilder, kClass: KClass<T>): T? {
        return this.queryForMap(nativeQueryBuilder)
            ?.let { objectMapper.convertValue(it, kClass.java) }
    }

    fun execute(nativeQueryBuilder: NativeQueryBuilder): Int {
        return namedParameterJdbcTemplate.update(nativeQueryBuilder.query, nativeQueryBuilder.queryParams)
    }

    fun batchExecute(nativeQueryBuilder: NativeQueryBuilder): Int {
        return namedParameterJdbcTemplate.batchUpdate(
            nativeQueryBuilder.query,
            nativeQueryBuilder.batchQueryParams.toTypedArray()
        ).sum()
    }
}

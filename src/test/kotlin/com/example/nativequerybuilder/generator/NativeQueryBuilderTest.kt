package com.example.nativequerybuilder.generator

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.ui.context.support.UiApplicationContextUtils
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.random.Random

@SpringBootTest
class NativeQueryBuilderTest {

    @Autowired
    lateinit var namedParameterJdbcTemplate: NamedParameterJdbcTemplate

    @BeforeEach
    fun setUp() {
        namedParameterJdbcTemplate.jdbcTemplate.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                created_at TIMESTAMP with time zone,
                last_modified_at TIMESTAMP with time zone
             );
            
              
        CREATE TABLE IF NOT EXISTS orders (
            id UUID PRIMARY KEY,
            user_id UUID,
            product_name VARCHAR(255),
            quantity INT,
            price int,
            order_date TIMESTAMP with time zone
        );
        """.trimIndent()
        )


    }

    @Test
    fun insertUserTest() {
        insertUser()
    }

    private fun insertUser(): UUID {
        val id = UUID.randomUUID()
        NativeQueryBuilder()
            .insert("users")
            .columns("id", "name", "email", "created_at", "last_modified_at")
            .values(":id", ":name", ":email", ":createdAt", ":lastModifiedAt")
            .addParams(
                "id" to id,
                "name" to "John",
                "email" to "jgm@email.com",
                "createdAt" to OffsetDateTime.now(),
                "lastModifiedAt" to OffsetDateTime.now()
            )
            .let {
                namedParameterJdbcTemplate.update(it.query, it.queryParams)
            }

        return id
    }

    fun insertOrder(userId: UUID) {
        NativeQueryBuilder()
            .insert("orders")
            .columns("id", "user_id", "product_name", "quantity", "price", "order_date")
            .values(":id", ":userId", ":productName", ":quantity", ":price", ":orderDate")
            .addParams(
                "id" to UUID.randomUUID(),
                "userId" to userId,
                "productName" to "${userId}_Macbook Pro",
                "quantity" to 1,
                "price" to Random.nextInt(100, 1000),
                "orderDate" to OffsetDateTime.now()
            )
            .let {
                namedParameterJdbcTemplate.update(it.query, it.queryParams)
            }
    }

    @Test
    fun selectAllUsers() {
        repeat(10) {
            val userid = insertUser()
            repeat(Random.nextInt(1, 5)) {
                insertOrder(userid)
            }
        }

        NativeQueryBuilder()
            .with("ordersCountByUser") {
                select(
                    "user_id",
                    "count(*) as order_count",
                    "sum(price) as total_price",
                    "max(order_date) as last_order_date"
                )
                    .from("orders")
                    .groupBy("user_id")
                    .having("count(*) > 2").and("total_price >1000")
            }
            .select(
                "users.id as userId",
                "name",
                "email",
                "order_count",
                "total_price",
                "to_char(ordersCountByUser.last_order_date, 'YYYY-MM-DD') as last_order_date"
            )
            .from("users")
            .join("ordersCountByUser").on("users.id = ordersCountByUser.user_id")
            .orderBy("ordersCountByUser.order_count desc", "ordersCountByUser.total_price desc")
            .limit(3)
            .let { namedParameterJdbcTemplate.queryForList(it.query, it.queryParams) }
            .forEach {
                println(it)
            }
    }

}

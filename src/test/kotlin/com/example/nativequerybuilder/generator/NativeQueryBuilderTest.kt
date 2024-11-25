package com.example.nativequerybuilder.generator

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.OffsetDateTime
import java.util.UUID
import kotlin.random.Random

@SpringBootTest
class NativeQueryBuilderTest {

    @Autowired
    lateinit var namedParameterJdbcTemplate: NamedParameterJdbcTemplate

    @Autowired
    lateinit var nativeQueryTemplate: NativeQueryTemplate

    @BeforeEach
    fun setUp() {
        namedParameterJdbcTemplate.jdbcTemplate.execute(
            """
            CREATE TABLE IF NOT EXISTS stores (
                id UUID PRIMARY KEY,
                name VARCHAR,
                rate_plan_kind VARCHAR,
                created_at TIMESTAMP with time zone,
                last_modified_at TIMESTAMP with time zone
             );
            
              
            CREATE TABLE IF NOT EXISTS settlement (
                id UUID PRIMARY KEY,
                store_id UUID,
                payment_method VARCHAR,
                payment_price INT,
                promotion_price INT,
                sales_date TIMESTAMP with time zone
            );
        """.trimIndent()
        )
    }

    fun insertStore(
        ratePlanKind: String,
        offsetDateTime: OffsetDateTime,
    ): UUID {
        val storeId: UUID = UUID.randomUUID()
        NativeQueryBuilder()
            .insert("stores")
            .columns("id", "name", "rate_plan_kind", "created_at", "last_modified_at")
            .values(
                ":id",
                ":name",
                ":rate_plan_kind",
                ":created_at",
                ":last_modified_at"
            )
            .addParams(
                "id" to storeId,
                "name" to "store_${storeId}",
                "rate_plan_kind" to ratePlanKind,
                "created_at" to offsetDateTime,
                "last_modified_at" to offsetDateTime
            ).let { nativeQueryTemplate.execute(it) }

        return storeId
    }

    fun insertSettlements() {
        val storeIds = mutableListOf<UUID>()

        repeat(100) { index ->
            if (index % 2 == 0) {
                insertStore("FLAT", OffsetDateTime.now().minusDays(index.toLong()))
                    .let { storeIds.add(it) }
            } else {
                insertStore("BROKERAGE", OffsetDateTime.now().minusDays(index.toLong()))
                    .let { storeIds.add(it) }
            }
        }


        val paymentMethods = listOf("CARD", "CASH", "MOBILE")
        val salesDates = listOf(
            OffsetDateTime.now().minusDays(1),
            OffsetDateTime.now().minusDays(2),
            OffsetDateTime.now().minusDays(3)
        )

        NativeQueryBuilder()
            .insert("settlement")
            .columns("id", "store_id", "payment_method", "payment_price", "promotion_price", "sales_date")
            .values(
                ":id",
                ":store_id",
                ":payment_method",
                ":payment_price",
                ":promotion_price",
                ":sales_date"
            )
            .mapLoop(storeIds) { storeId ->
                addBatchParams(
                    "id" to UUID.randomUUID(),
                    "store_id" to storeId,
                    "payment_method" to paymentMethods[Random.nextInt(0, 3)],
                    "payment_price" to Random.nextInt(1000, 10000),
                    "promotion_price" to Random.nextInt(100, 1000),
                    "sales_date" to salesDates[Random.nextInt(0, 3)]
                )
            }.let { nativeQueryTemplate.batchExecute(it) }
    }

    @Test
    fun `query settlements`() {
        insertSettlements()

        NativeQueryBuilder()
            .with("settlementGroupByStoreIdAndSalesDateAndPaymentMethod"){
                select("store_id", "sales_date", "payment_method")
                    .from("settlement")
                    .join("stores").on("settlement.store_id = stores.id")
            }

    }
}

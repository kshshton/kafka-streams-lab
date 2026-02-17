import java.util.Properties
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver

class OrderProcessingTopologyTest {
    @Test
    fun paidOrderIsCleanedEnrichedAlertedAndAggregated() {
        withTestDriver("test-app-paid-order") {
            givenUserProfile("u1", "gold")
            givenOrder("u1", " ord-1, us-east , 1500.0 , usd , paid ")

            thenCleanOrderIs("ord-1|u1|us-east|1500.0|USD|PAID")
            thenEnrichedOrderIs("ord-1|u1|us-east|1500.0|USD|PAID|gold")
            thenHighValueAlertIs("HIGH_VALUE|ord-1|u1|1500.0|gold")
            thenRevenueIs("us-east", 1500.0)
            thenNoInvalidOrders()
        }
    }

    @Test
    fun invalidOrderGoesToInvalidTopicOnly() {
        withTestDriver("test-app-invalid-order") {
            givenOrder("u1", "bad-input")

            thenInvalidOrderContains("INVALID_FORMAT|bad-input")
            thenNoCleanOrders()
            thenNoEnrichedOrders()
            thenNoHighValueAlerts()
            thenNoRevenueUpdates()
        }
    }

    @Test
    fun nonPaidOrderIsCleanedButNotAggregatedOrEnriched() {
        withTestDriver("test-app-non-paid") {
            givenUserProfile("u2", "silver")
            givenOrder("u2", "ord-2,eu-west,25.5,eur,created")

            thenCleanOrderIs("ord-2|u2|eu-west|25.5|EUR|CREATED")
            thenNoEnrichedOrders()
            thenNoHighValueAlerts()
            thenNoRevenueUpdates()
            thenNoInvalidOrders()
        }
    }

    @Test
    fun revenueAggregationIsCumulativePerRegion() {
        withTestDriver("test-app-revenue") {
            givenOrder("u1", "ord-10,us-east,100.0,usd,paid")
            givenOrder("u2", "ord-11,us-east,50.0,usd,paid")

            thenRevenueIs("us-east", 100.0)
            thenRevenueIs("us-east", 150.0)
        }
    }

    private fun withTestDriver(
        appId: String,
        scenario: KafkaTestScenario.() -> Unit
    ) {
        val topology = buildTopology(StreamsBuilder())
        val properties = testProperties(appId)

        TopologyTestDriver(topology, properties).use { driver ->
            KafkaTestScenario(driver).scenario()
        }
    }

    private fun testProperties(appId: String): Properties =
        Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
        }

    private class KafkaTestScenario(driver: TopologyTestDriver) {
        private val stringSerde = Serdes.String()
        private val doubleSerde = Serdes.Double()

        private val ordersInputTopic = driver.createInputTopic(
            INPUT_TOPIC,
            stringSerde.serializer(),
            stringSerde.serializer()
        )
        private val userProfilesInputTopic = driver.createInputTopic(
            PROFILE_LOOKUP_TOPIC,
            stringSerde.serializer(),
            stringSerde.serializer()
        )
        private val cleanOrdersTopic = driver.createOutputTopic(
            CLEAN_ORDERS_TOPIC,
            stringSerde.deserializer(),
            stringSerde.deserializer()
        )
        private val invalidOrdersTopic = driver.createOutputTopic(
            INVALID_ORDERS_TOPIC,
            stringSerde.deserializer(),
            stringSerde.deserializer()
        )
        private val enrichedOrdersTopic = driver.createOutputTopic(
            ENRICHED_ORDERS_TOPIC,
            stringSerde.deserializer(),
            stringSerde.deserializer()
        )
        private val highValueAlertsTopic = driver.createOutputTopic(
            HIGH_VALUE_ALERTS_TOPIC,
            stringSerde.deserializer(),
            stringSerde.deserializer()
        )
        private val revenueByRegionTopic = driver.createOutputTopic(
            REVENUE_BY_REGION_TOPIC,
            stringSerde.deserializer(),
            doubleSerde.deserializer()
        )

        fun givenOrder(userId: String, rawOrder: String) = ordersInputTopic.pipeInput(userId, rawOrder)
        fun givenUserProfile(userId: String, tier: String) = userProfilesInputTopic.pipeInput(userId, tier)

        fun thenCleanOrderIs(expected: String) = assertEquals(expected, cleanOrdersTopic.readValue())
        fun thenInvalidOrderContains(expectedPrefix: String) =
            assertTrue(invalidOrdersTopic.readValue().startsWith(expectedPrefix))
        fun thenEnrichedOrderIs(expected: String) = assertEquals(expected, enrichedOrdersTopic.readValue())
        fun thenHighValueAlertIs(expected: String) = assertEquals(expected, highValueAlertsTopic.readValue())

        fun thenRevenueIs(expectedRegion: String, expectedAmount: Double) {
            val record = revenueByRegionTopic.readKeyValue()
            assertEquals(expectedRegion, record.key)
            assertEquals(expectedAmount, record.value)
        }

        fun thenNoCleanOrders() = assertTrue(cleanOrdersTopic.isEmpty)
        fun thenNoInvalidOrders() = assertTrue(invalidOrdersTopic.isEmpty)
        fun thenNoEnrichedOrders() = assertTrue(enrichedOrdersTopic.isEmpty)
        fun thenNoHighValueAlerts() = assertTrue(highValueAlertsTopic.isEmpty)
        fun thenNoRevenueUpdates() = assertTrue(revenueByRegionTopic.isEmpty)
    }
}

import java.util.Properties
import kotlin.test.Test
import kotlin.test.assertEquals
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver

class TopologyTest {
    @Test
    fun uppercaseFlow() {
        withTestDriver("test-app-uppercase") {
            givenInput("k1", "hello")
            thenUppercaseOutputIs("HELLO")
        }
    }

    @Test
    fun aggregationCountFlow() {
        withTestDriver("test-app-aggregation") {
            givenInput("k1", "a")
            givenInput("k1", "b")
            givenInput("k1", "c")

            thenAggregationCountIs(1L)
            thenAggregationCountIs(2L)
            thenAggregationCountIs(3L)
        }
    }

    @Test
    fun joinFlow() {
        withTestDriver("test-app-join") {
            givenLookup("k1", "user-1")
            givenInput("k1", "event-a")
            thenJoinOutputIs("event-a:user-1")
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
        private val inputTopic = driver.createInputTopic(
            INPUT_TOPIC,
            Serdes.String().serializer(),
            Serdes.String().serializer()
        )
        private val lookupTopic = driver.createInputTopic(
            LOOKUP_TOPIC,
            Serdes.String().serializer(),
            Serdes.String().serializer()
        )
        private val uppercaseOutputTopic = driver.createOutputTopic(
            OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.String().deserializer()
        )
        private val aggregationOutputTopic = driver.createOutputTopic(
            AGGREGATION_OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.Long().deserializer()
        )
        private val joinOutputTopic = driver.createOutputTopic(
            JOIN_OUTPUT_TOPIC,
            Serdes.String().deserializer(),
            Serdes.String().deserializer()
        )

        fun givenInput(key: String, value: String) = inputTopic.pipeInput(key, value)
        fun givenLookup(key: String, value: String) = lookupTopic.pipeInput(key, value)
        fun thenUppercaseOutputIs(expected: String) = assertEquals(expected, uppercaseOutputTopic.readValue())
        fun thenAggregationCountIs(expected: Long) = assertEquals(expected, aggregationOutputTopic.readValue())
        fun thenJoinOutputIs(expected: String) = assertEquals(expected, joinOutputTopic.readValue())
    }
}

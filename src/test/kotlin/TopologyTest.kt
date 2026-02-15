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
        val topology = buildTopology(StreamsBuilder())

        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
        }

        TopologyTestDriver(/* topology = */ topology, /* config = */ properties).use { driver ->
            val inputTopic = driver.createInputTopic(
                /* topicName = */ INPUT_TOPIC,
                /* keySerializer = */ Serdes.String().serializer(),
                /* valueSerializer = */ Serdes.String().serializer()
            )
            val outputTopic = driver.createOutputTopic(
                /* topicName = */ OUTPUT_TOPIC,
                /* keyDeserializer = */ Serdes.String().deserializer(),
                /* valueDeserializer = */ Serdes.String().deserializer()
            )

            inputTopic.pipeInput("k1", "hello")

            assertEquals(expected = "HELLO", actual = outputTopic.readValue())
        }
    }

    @Test
    fun aggregationCountFlow() {
        val topology = buildTopology(StreamsBuilder())

        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-aggregation")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
        }

        TopologyTestDriver(/* topology = */ topology, /* config = */ properties).use { driver ->
            val inputTopic = driver.createInputTopic(
                /* topicName = */ INPUT_TOPIC,
                /* keySerializer = */ Serdes.String().serializer(),
                /* valueSerializer = */ Serdes.String().serializer()
            )
            val aggregationOutputTopic = driver.createOutputTopic(
                /* topicName = */ AGGREGATION_OUTPUT_TOPIC,
                /* keyDeserializer = */ Serdes.String().deserializer(),
                /* valueDeserializer = */ Serdes.Long().deserializer()
            )

            inputTopic.pipeInput("k1", "a")
            inputTopic.pipeInput("k1", "b")
            inputTopic.pipeInput("k1", "c")

            assertEquals(expected = 1L, actual = aggregationOutputTopic.readValue())
            assertEquals(expected = 2L, actual = aggregationOutputTopic.readValue())
            assertEquals(expected = 3L, actual = aggregationOutputTopic.readValue())
        }
    }

    @Test
    fun joinFlow() {
        val topology = buildTopology(StreamsBuilder())

        val properties = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app-join")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092")
            put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
            put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde::class.java)
        }

        TopologyTestDriver(/* topology = */ topology, /* config = */ properties).use { driver ->
            val inputTopic = driver.createInputTopic(
                /* topicName = */ INPUT_TOPIC,
                /* keySerializer = */ Serdes.String().serializer(),
                /* valueSerializer = */ Serdes.String().serializer()
            )
            val lookupTopic = driver.createInputTopic(
                /* topicName = */ LOOKUP_TOPIC,
                /* keySerializer = */ Serdes.String().serializer(),
                /* valueSerializer = */ Serdes.String().serializer()
            )
            val joinOutputTopic = driver.createOutputTopic(
                /* topicName = */ JOIN_OUTPUT_TOPIC,
                /* keyDeserializer = */ Serdes.String().deserializer(),
                /* valueDeserializer = */ Serdes.String().deserializer()
            )

            lookupTopic.pipeInput("k1", "user-1")
            inputTopic.pipeInput("k1", "event-a")

            assertEquals(expected = "event-a:user-1", actual = joinOutputTopic.readValue())
        }
    }
}

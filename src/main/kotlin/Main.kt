import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced

const val INPUT_TOPIC = "in"
const val OUTPUT_TOPIC = "out"
const val AGGREGATION_OUTPUT_TOPIC = "out-counts"
const val LOOKUP_TOPIC = "in-lookup"
const val JOIN_OUTPUT_TOPIC = "out-joined"

fun addUppercaseFlow(source: KStream<String, String>) {
    source
        .mapValues { it.uppercase() }
        .to(OUTPUT_TOPIC)
}

fun addAggregationFlow(source: KStream<String, String>) {
    source
        .groupByKey()
        .count()
        .toStream()
        .to(AGGREGATION_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()))
}

fun addJoinFlow(builder: StreamsBuilder, source: KStream<String, String>) {
    val lookupTable = builder.table<String, String>(LOOKUP_TOPIC)
    source
        .join(lookupTable) { leftValue, rightValue -> "$leftValue:$rightValue" }
        .to(JOIN_OUTPUT_TOPIC)
}

fun buildTopology(builder: StreamsBuilder): Topology {
    val source = builder.stream<String, String>(INPUT_TOPIC)
    addUppercaseFlow(source)
    addAggregationFlow(source)
    addJoinFlow(builder, source)

    return builder.build()
}

fun main() {
    val topology = buildTopology(StreamsBuilder())
    println(topology.describe())
}

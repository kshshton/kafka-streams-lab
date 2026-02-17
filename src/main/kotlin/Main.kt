import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced

const val INPUT_TOPIC = "orders-in"
const val PROFILE_LOOKUP_TOPIC = "user-profiles"
const val CLEAN_ORDERS_TOPIC = "orders-clean"
const val INVALID_ORDERS_TOPIC = "orders-invalid"
const val ENRICHED_ORDERS_TOPIC = "orders-enriched"
const val HIGH_VALUE_ALERTS_TOPIC = "high-value-alerts"
const val REVENUE_BY_REGION_TOPIC = "revenue-by-region"
const val HIGH_VALUE_THRESHOLD = 1000.0

data class OrderEvent(
    val orderId: String,
    val userId: String,
    val region: String,
    val amount: Double,
    val currency: String,
    val status: String
) {
    fun toCanonicalString(): String =
        "$orderId|$userId|$region|$amount|$currency|$status"
}

sealed class OrderParseResult {
    data class Valid(val order: OrderEvent) : OrderParseResult()
    data class Invalid(val reason: String, val raw: String) : OrderParseResult()
}

fun addNormalizationStage(source: KStream<String, String>): KStream<String, String> =
    source
        .mapValues { it.trim() }
        .filter { _, value -> value.isNotEmpty() }

fun parseOrder(userId: String, rawValue: String): OrderParseResult {
    val parts = rawValue.split(",").map { it.trim() }
    if (parts.size != 5) {
        return OrderParseResult.Invalid("INVALID_FORMAT", rawValue)
    }

    val (orderId, rawRegion, rawAmount, rawCurrency, rawStatus) = parts
    val region = rawRegion.lowercase()
    val amount = rawAmount.toDoubleOrNull()
    val currency = rawCurrency.uppercase()
    val status = rawStatus.uppercase()

    if (orderId.isBlank()) return OrderParseResult.Invalid("MISSING_ORDER_ID", rawValue)
    if (region.isBlank()) return OrderParseResult.Invalid("MISSING_REGION", rawValue)
    if (amount == null || amount <= 0.0) return OrderParseResult.Invalid("INVALID_AMOUNT", rawValue)
    if (currency.length != 3) return OrderParseResult.Invalid("INVALID_CURRENCY", rawValue)

    return OrderParseResult.Valid(
        OrderEvent(
            orderId = orderId,
            userId = userId,
            region = region,
            amount = amount,
            currency = currency,
            status = status
        )
    )
}

fun formatInvalid(result: OrderParseResult.Invalid): String =
    "${result.reason}|${result.raw}"

fun addValidationAndParsingStage(source: KStream<String, String>): KStream<String, OrderEvent> {
    val parsed = source.mapValues { key, value -> parseOrder(key, value) }
    val branches = parsed
        .split(Named.`as`("orders-"))
        .branch({ _, result -> result is OrderParseResult.Valid }, Branched.`as`("valid"))
        .defaultBranch(Branched.`as`("invalid"))

    val invalidBranch = requireNotNull(branches["orders-invalid"])
    invalidBranch
        .mapValues { result -> formatInvalid(result as OrderParseResult.Invalid) }
        .to(INVALID_ORDERS_TOPIC)

    val validBranch = requireNotNull(branches["orders-valid"])
    return validBranch
        .mapValues { result -> (result as OrderParseResult.Valid).order }
}

fun addCleanOrdersFlow(source: KStream<String, OrderEvent>) {
    source
        .mapValues { order -> order.toCanonicalString() }
        .to(CLEAN_ORDERS_TOPIC)
}

fun addEnrichmentFlow(userProfiles: KTable<String, String>, source: KStream<String, OrderEvent>) {
    source
        .filter { _, order -> order.status == "PAID" }
        .leftJoin(userProfiles) { order, tier ->
            val safeTier = tier ?: "unknown"
            "${order.toCanonicalString()}|$safeTier"
        }
        .to(ENRICHED_ORDERS_TOPIC)
}

fun addHighValueAlertFlow(userProfiles: KTable<String, String>, source: KStream<String, OrderEvent>) {
    source
        .filter { _, order -> order.status == "PAID" && order.amount >= HIGH_VALUE_THRESHOLD }
        .leftJoin(userProfiles) { order, tier ->
            val safeTier = tier ?: "unknown"
            "HIGH_VALUE|${order.orderId}|${order.userId}|${order.amount}|$safeTier"
        }
        .to(HIGH_VALUE_ALERTS_TOPIC)
}

fun addRevenueAggregationFlow(source: KStream<String, OrderEvent>) {
    source
        .filter { _, order -> order.status == "PAID" }
        .map { _, order -> KeyValue.pair(order.region, order.amount) }
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
        .reduce(Double::plus)
        .toStream()
        .to(REVENUE_BY_REGION_TOPIC, Produced.with(Serdes.String(), Serdes.Double()))
}

fun buildExamplePipeline(builder: StreamsBuilder): Topology {
    val source = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
    val userProfiles = builder.table<String, String>(PROFILE_LOOKUP_TOPIC)
    val normalized = addNormalizationStage(source)
    val validOrders = addValidationAndParsingStage(normalized)

    addCleanOrdersFlow(validOrders)
    addEnrichmentFlow(userProfiles, validOrders)
    addHighValueAlertFlow(userProfiles, validOrders)
    addRevenueAggregationFlow(validOrders)

    return builder.build()
}

fun buildTopology(builder: StreamsBuilder): Topology = buildExamplePipeline(builder)

fun main() {
    val topology = buildExamplePipeline(StreamsBuilder())
    println(topology.describe())
}

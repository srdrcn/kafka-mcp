package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeProducersResult;
import org.apache.kafka.clients.admin.DescribeTransactionsResult;
import org.apache.kafka.clients.admin.ListTransactionsOptions;
import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Producer ve transaction durumu: tek describeProducers, listTransactions + tek describeTransactions. */
public final class Producers {

	/** Coordinator'da hâlâ açık sayılan transaction durumları. */
	public static final Set<TransactionState> OPEN_STATES = Set.of(TransactionState.ONGOING,
			TransactionState.PREPARE_COMMIT, TransactionState.PREPARE_ABORT, TransactionState.PREPARE_EPOCH_FENCE);

	/** transaction.max.timeout.ms'in Kafka varsayılanı (15 dk); broker config'i okunamazsa kullanılır. */
	public static final long DEFAULT_MAX_TRANSACTION_TIMEOUT_MS = 900_000;

	/** Açık transaction listesi ve yanıt vermeyen coordinator broker'larının hataları. */
	public record OpenTransactions(List<TransactionListing> listings, Map<Integer, String> errors) {
	}

	private Producers() {
	}

	/** Partition başına aktif producer'lar (tek describeProducers); partition bazında hatalar ayrı. */
	public static Futures.Each<TopicPartition, List<ProducerState>> describe(ToolCall call,
			Collection<TopicPartition> tps) {
		if (tps.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		DescribeProducersResult r = call.admin().describeProducers(tps);
		Map<TopicPartition, KafkaFuture<List<ProducerState>>> futures = new LinkedHashMap<>();
		for (TopicPartition tp : tps) {
			futures.put(tp, r.partitionResult(tp).thenApply(DescribeProducersResult.PartitionProducerState::activeProducers));
		}
		return Futures.awaitEach(call, futures);
	}

	/** Producer durumunun kompakt gösterimi (zaman ISO-8601). */
	public static Map<String, Object> row(TopicPartition tp, ProducerState s) {
		return Results.map("partition", tp.partition(), "producerId", s.producerId(), "producerEpoch",
				s.producerEpoch(), "lastSequence", s.lastSequence(), "lastTimestamp", Format.iso(s.lastTimestamp()),
				"coordinatorEpoch", s.coordinatorEpoch().isPresent() ? s.coordinatorEpoch().getAsInt() : null,
				"currentTransactionStartOffset",
				s.currentTransactionStartOffset().isPresent() ? s.currentTransactionStartOffset().getAsLong() : null);
	}

	/**
	 * Açık transaction listesi (tüm coordinator'lardan, sadece açık durumlar). Broker bazında beklenir; bir
	 * coordinator yanıt vermezse diğerlerininki döner, hata ayrıca raporlanır.
	 */
	public static OpenTransactions listOpen(ToolCall call) {
		Map<Integer, KafkaFuture<Collection<TransactionListing>>> byBroker = call
			.await(call.admin().listTransactions(new ListTransactionsOptions().filterStates(OPEN_STATES)).byBrokerId());
		Futures.Each<Integer, Collection<TransactionListing>> each = Futures.awaitEach(call, byBroker);
		List<TransactionListing> listings = new ArrayList<>();
		each.values().values().forEach(listings::addAll);
		return new OpenTransactions(listings, new TreeMap<>(each.errors()));
	}

	/**
	 * Askıda (hanging) transaction şüphesi, kafka-transactions.sh find-hanging mantığıyla: partition'da açık
	 * transaction var, producer'ın son yazması {@code maxTimeoutMs}'den (transaction.max.timeout.ms) eski ve
	 * coordinator'da producer'ın açık transaction'ı yok. Yeni başlamış transaction'lar yaşları nedeniyle elenir.
	 */
	public static boolean hangingSuspect(ProducerState s, Set<Long> openProducerIds, long maxTimeoutMs, long nowMs) {
		return s.currentTransactionStartOffset().isPresent() && s.lastTimestamp() < nowMs - maxTimeoutMs
				&& !openProducerIds.contains(s.producerId());
	}

	/** Broker'ın transaction.max.timeout.ms değeri; okunamazsa {@link #DEFAULT_MAX_TRANSACTION_TIMEOUT_MS}. */
	public static long maxTransactionTimeoutMs(ToolCall call) {
		try {
			ClusterMeta.Info info = ClusterMeta.describe(call, false);
			ConfigEntry e = Configs.describeOne(call, Configs.broker(info.defaultBrokerId()))
				.get("transaction.max.timeout.ms");
			if (e != null && e.value() != null) {
				return Long.parseLong(e.value());
			}
		}
		catch (ToolException | NumberFormatException e) {
			// varsayılana düş
		}
		return DEFAULT_MAX_TRANSACTION_TIMEOUT_MS;
	}

	/** Verilen transactional id'ler için tek describeTransactions; id bazında hatalar ayrı. */
	public static Futures.Each<String, TransactionDescription> describeTransactions(ToolCall call,
			Collection<String> transactionalIds) {
		if (transactionalIds.isEmpty()) {
			return new Futures.Each<>(new LinkedHashMap<>(), new LinkedHashMap<>());
		}
		DescribeTransactionsResult r = call.admin().describeTransactions(transactionalIds);
		Map<String, KafkaFuture<TransactionDescription>> futures = new LinkedHashMap<>();
		for (String id : transactionalIds) {
			futures.put(id, r.description(id));
		}
		return Futures.awaitEach(call, futures);
	}

}

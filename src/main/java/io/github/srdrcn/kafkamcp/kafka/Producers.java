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

/** Producer and transaction state: a single describeProducers, listTransactions + a single describeTransactions. */
public final class Producers {

	/** Transaction states that the coordinator still counts as open. */
	public static final Set<TransactionState> OPEN_STATES = Set.of(TransactionState.ONGOING,
			TransactionState.PREPARE_COMMIT, TransactionState.PREPARE_ABORT, TransactionState.PREPARE_EPOCH_FENCE);

	/** Kafka default of transaction.max.timeout.ms (15 min); used if the broker config cannot be read. */
	public static final long DEFAULT_MAX_TRANSACTION_TIMEOUT_MS = 900_000;

	/** List of open transactions and the errors of coordinator brokers that did not respond. */
	public record OpenTransactions(List<TransactionListing> listings, Map<Integer, String> errors) {
	}

	private Producers() {
	}

	/** Active producers per partition (a single describeProducers); per-partition errors separately. */
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

	/** Compact representation of a producer state (time as ISO-8601). */
	public static Map<String, Object> row(TopicPartition tp, ProducerState s) {
		return Results.map("partition", tp.partition(), "producerId", s.producerId(), "producerEpoch",
				s.producerEpoch(), "lastSequence", s.lastSequence(), "lastTimestamp", Format.iso(s.lastTimestamp()),
				"coordinatorEpoch", s.coordinatorEpoch().isPresent() ? s.coordinatorEpoch().getAsInt() : null,
				"currentTransactionStartOffset",
				s.currentTransactionStartOffset().isPresent() ? s.currentTransactionStartOffset().getAsLong() : null);
	}

	/**
	 * List of open transactions (from all coordinators, open states only). Awaited per broker; if a coordinator
	 * does not respond, the others' results are returned and the error is reported separately.
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
	 * Suspected hanging transaction, following the kafka-transactions.sh find-hanging logic: the partition has an
	 * open transaction, the producer's last write is older than {@code maxTimeoutMs} (transaction.max.timeout.ms)
	 * and the coordinator has no open transaction for the producer. Recently started transactions are ruled out by
	 * their age.
	 */
	public static boolean hangingSuspect(ProducerState s, Set<Long> openProducerIds, long maxTimeoutMs, long nowMs) {
		return s.currentTransactionStartOffset().isPresent() && s.lastTimestamp() < nowMs - maxTimeoutMs
				&& !openProducerIds.contains(s.producerId());
	}

	/** transaction.max.timeout.ms of the broker; {@link #DEFAULT_MAX_TRANSACTION_TIMEOUT_MS} if unreadable. */
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
			// fall back to the default
		}
		return DEFAULT_MAX_TRANSACTION_TIMEOUT_MS;
	}

	/** A single describeTransactions for the given transactional ids; per-id errors separately. */
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

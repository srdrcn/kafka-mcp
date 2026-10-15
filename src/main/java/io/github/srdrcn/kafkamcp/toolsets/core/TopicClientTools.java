package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.kafka.clients.admin.ProducerState;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionListing;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;

import io.github.srdrcn.kafkamcp.connect.ConnectClient;
import io.github.srdrcn.kafkamcp.kafka.ConsumerGroups;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.kafka.Producers;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Clients of a topic: consumer groups, Connect connectors, producers, transactions. */
@Component
public class TopicClientTools {

	/** Note for brokers that do not support describeProducers (before Kafka 3.0). */
	static final String UNSUPPORTED_PRODUCERS = KafkaErrors.UNSUPPORTED
			+ ": describeProducers requires Kafka 3.0+ brokers (producers of these partitions could not be read)";

	/** At most this many concurrent requests to the Connect REST API. */
	private static final int CONNECT_PARALLELISM = 8;

	private final ConnectClient connect;

	public TopicClientTools(ConnectClient connect) {
		this.connect = connect;
	}

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("topics_consumers",
						"Consumer groups with committed offsets on the topic: type (Classic/Consumer), state, member count, lag on this topic")
					.required("topic", P.string("Topic name"))
					.listParams()
					.handler(this::topicsConsumers),
				ToolSpec.read("topics_connectors",
						"Connect connectors that use the topic (active topics, or topic/topics/regex/prefix in the config)")
					.connect()
					.required("topic", P.string("Topic name"))
					.handler(this::topicsConnectors),
				ToolSpec.read("topics_producers",
						"Active producers on the topic partitions: producerId, epoch, last sequence/timestamp, open transaction")
					.required("topic", P.string("Topic name"))
					.param("limit", Params.limit(ToolSpec.DEFAULT_LIST_LIMIT))
					.handler(this::topicsProducers),
				ToolSpec.read("topics_transactions",
						"Open transactions touching the topic, with hints of possibly hanging transactions")
					.required("topic", P.string("Topic name"))
					.handler(this::topicsTransactions));
	}

	Object topicsConsumers(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		ConsumerGroups.TopicConsumers tc = ConsumerGroups.forTopic(call, d);
		List<Map<String, Object>> rows = new ArrayList<>();
		long totalLag = 0;
		for (ConsumerGroups.TopicLag g : tc.groups()) {
			if (!call.matches(g.groupId())) {
				continue;
			}
			totalLag += g.lag();
			rows.add(Results.map("groupId", g.groupId(), "type", g.type(), "state", g.state(), "members",
					g.members(), "lag", g.lag(), "partitions", g.partitions()));
		}
		Map<String, Object> out = Results.map("topic", topic, "groupsScanned", tc.groupsScanned(), "totalLag",
				totalLag, "failedGroups", tc.errors().isEmpty() ? null : tc.errors().size(), "lagUnknownPartitions",
				tc.lagUnknownPartitions() > 0 ? tc.lagUnknownPartitions() : null);
		if (!tc.errors().isEmpty() || tc.lagUnknownPartitions() > 0) {
			out.put("partial", true);
		}
		out.putAll(Results.page("groups", rows, call.limit()));
		return out;
	}

	Object topicsConnectors(ToolCall call) throws Exception {
		String topic = call.str("topic");
		String cluster = call.cluster();
		JsonNode all = connect.get(cluster, "/connectors?expand=info&expand=status");
		Map<String, Map<String, Object>> found = new LinkedHashMap<>();
		Map<String, List<String>> matchedBy = new LinkedHashMap<>();
		List<String> names = new ArrayList<>();
		Iterator<Map.Entry<String, JsonNode>> it = all.fields();
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> e = it.next();
			String name = e.getKey();
			names.add(name);
			JsonNode info = e.getValue().path("info");
			JsonNode status = e.getValue().path("status");
			found.put(name, Results.map("name", name, "type", text(info.path("type")), "state",
					text(status.path("connector").path("state")), "tasks", status.path("tasks").size()));
			matchedBy.put(name, new ArrayList<>(configMatches(info.path("config"), topic)));
		}
		int activeErrors = 0;
		int unchecked = 0;
		try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
			Semaphore permits = new Semaphore(CONNECT_PARALLELISM);
			Map<String, Future<JsonNode>> futures = new LinkedHashMap<>();
			for (String name : names) {
				futures.put(name, pool.submit(() -> {
					permits.acquire();
					try {
						return connect.get(cluster, "/connectors/" + ConnectClient.enc(name) + "/topics");
					}
					finally {
						permits.release();
					}
				}));
			}
			for (Map.Entry<String, Future<JsonNode>> f : futures.entrySet()) {
				try {
					JsonNode active = f.getValue().get(Math.max(1, call.waitMillis() - 500), TimeUnit.MILLISECONDS);
					for (JsonNode t : active.path(f.getKey()).path("topics")) {
						if (topic.equals(t.asText())) {
							matchedBy.get(f.getKey()).add(0, "activeTopics");
						}
					}
				}
				catch (TimeoutException e) {
					f.getValue().cancel(true);
					unchecked++;
				}
				catch (ExecutionException e) {
					activeErrors++;
				}
			}
			pool.shutdownNow();
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		matchedBy.forEach((name, by) -> {
			if (!by.isEmpty()) {
				Map<String, Object> row = found.get(name);
				row.put("matchedBy", by);
				rows.add(row);
			}
		});
		return Results.map("topic", topic, "connectorsScanned", names.size(), "activeTopicsErrors",
				activeErrors > 0 ? activeErrors : null, "partial", unchecked > 0 ? true : null,
				"activeTopicsUnchecked", unchecked > 0 ? unchecked : null, "connectors", rows);
	}

	/**
	 * Config keys that refer to the topic: keys whose name contains "topic" and whose (comma-separated) value
	 * lists the topic, whose *regex/*pattern matches it, or whose *prefix it starts with.
	 */
	static List<String> configMatches(JsonNode config, String topic) {
		List<String> out = new ArrayList<>();
		Iterator<Map.Entry<String, JsonNode>> it = config.fields();
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> e = it.next();
			String key = e.getKey().toLowerCase();
			String value = e.getValue().asText("").trim();
			if (!key.contains("topic") || value.isEmpty()) {
				continue;
			}
			boolean match = false;
			if (key.endsWith("regex") || key.endsWith("pattern")) {
				try {
					match = Pattern.compile(value).matcher(topic).matches();
				}
				catch (PatternSyntaxException ignored) {
					// invalid regex: no match
				}
			}
			else if (key.endsWith("prefix")) {
				match = topic.startsWith(value);
			}
			else {
				for (String part : value.split(",")) {
					if (part.trim().equals(topic)) {
						match = true;
						break;
					}
				}
			}
			if (match) {
				out.add("config:" + e.getKey());
			}
		}
		return out;
	}

	Object topicsProducers(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		List<TopicPartition> online = Topics.onlinePartitions(List.of(d));
		Futures.Each<TopicPartition, List<ProducerState>> each = Producers.describe(call, online);
		// describeProducers requires Kafka 3.0+: partitions on older brokers go into a note, the tool does not fail
		Map<TopicPartition, String> failed = new LinkedHashMap<>();
		int unsupported = 0;
		for (Map.Entry<TopicPartition, String> e : each.errors().entrySet()) {
			if (KafkaErrors.isUnsupported(e.getValue())) {
				unsupported++;
			}
			else {
				failed.put(e.getKey(), e.getValue());
			}
		}
		if (each.values().isEmpty() && !failed.isEmpty()) {
			throw new ToolException(failed.values().iterator().next());
		}
		List<Map.Entry<TopicPartition, ProducerState>> states = new ArrayList<>();
		each.values().forEach((tp, list) -> list.forEach(s -> states.add(Map.entry(tp, s))));
		states.sort(Comparator.comparingLong((Map.Entry<TopicPartition, ProducerState> e) -> e.getValue().lastTimestamp())
			.reversed());
		Set<Long> ids = new HashSet<>();
		Set<TopicPartition> openTxnPartitions = new HashSet<>();
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map.Entry<TopicPartition, ProducerState> e : states) {
			ids.add(e.getValue().producerId());
			if (e.getValue().currentTransactionStartOffset().isPresent()) {
				openTxnPartitions.add(e.getKey());
			}
			rows.add(Producers.row(e.getKey(), e.getValue()));
		}
		Map<String, Object> out = Results.map("topic", topic, "partitionsScanned", online.size(), "producerCount",
				ids.size(), "partitionsWithOpenTransaction", openTxnPartitions.size(), "failedPartitions",
				failed.isEmpty() ? null : failed.size(), "unsupportedPartitions", unsupported > 0 ? unsupported : null,
				"note", unsupported > 0 ? UNSUPPORTED_PRODUCERS : null);
		out.putAll(Results.page("producers", rows, call.limit()));
		return out;
	}

	Object topicsTransactions(ToolCall call) {
		String topic = call.str("topic");
		TopicDescription d = Topics.describeOne(call, topic);
		// partitions first, then the coordinator: a transaction started in between shows up as open on the coordinator
		Futures.Each<TopicPartition, List<ProducerState>> producers = Producers
			.describe(call, Topics.onlinePartitions(List.of(d)));
		Producers.OpenTransactions open = Producers.listOpen(call);
		Set<Long> openProducerIds = new HashSet<>();
		List<String> ids = new ArrayList<>();
		for (TransactionListing l : open.listings()) {
			openProducerIds.add(l.producerId());
			ids.add(l.transactionalId());
		}
		Futures.Each<String, TransactionDescription> described = Producers.describeTransactions(call, ids);
		List<Map<String, Object>> txns = new ArrayList<>();
		described.values().forEach((id, t) -> {
			List<Integer> parts = t.topicPartitions()
				.stream()
				.filter(tp -> tp.topic().equals(topic))
				.map(TopicPartition::partition)
				.sorted()
				.toList();
			if (parts.isEmpty()) {
				return;
			}
			long start = t.transactionStartTimeMs().orElse(-1);
			Long age = Format.ageSeconds(start);
			txns.add(Results.map("transactionalId", id, "state", String.valueOf(t.state()), "producerId",
					t.producerId(), "producerEpoch", t.producerEpoch(), "startTime", Format.iso(start), "ageSeconds",
					age, "timeoutMs", t.transactionTimeoutMs(), "timedOut",
					age != null && age * 1000 > t.transactionTimeoutMs() ? true : null, "partitions", parts));
		});
		Map<TopicPartition, List<ProducerState>> byPartition = new TreeMap<>(
				Comparator.comparingInt(TopicPartition::partition));
		byPartition.putAll(producers.values());
		// the threshold is read only if an open transaction is missing on the coordinator (extra describeConfigs)
		boolean candidates = byPartition.values()
			.stream()
			.flatMap(List::stream)
			.anyMatch(s -> s.currentTransactionStartOffset().isPresent() && !openProducerIds.contains(s.producerId()));
		Long thresholdMs = candidates ? Producers.maxTransactionTimeoutMs(call) : null;
		long now = System.currentTimeMillis();
		List<Map<String, Object>> partitions = new ArrayList<>();
		int hanging = 0;
		for (Map.Entry<TopicPartition, List<ProducerState>> e : byPartition.entrySet()) {
			for (ProducerState s : e.getValue()) {
				if (s.currentTransactionStartOffset().isEmpty()) {
					continue;
				}
				boolean suspect = thresholdMs != null && Producers.hangingSuspect(s, openProducerIds, thresholdMs, now);
				hanging += suspect ? 1 : 0;
				partitions.add(Results.map("partition", e.getKey().partition(), "producerId", s.producerId(),
						"producerEpoch", s.producerEpoch(), "startOffset", s.currentTransactionStartOffset().getAsLong(),
						"lastTimestamp", Format.iso(s.lastTimestamp()), "ageSeconds", Format.ageSeconds(s.lastTimestamp()),
						"hangingSuspect", suspect ? true : null));
			}
		}
		Map<String, Object> out = Results.map("topic", topic, "openTransactionsInCluster", open.listings().size(),
				"openTransactions", txns, "partitionsWithOpenTransaction", partitions, "hangingSuspects", hanging,
				"hangingThresholdMs", thresholdMs);
		List<String> notes = new ArrayList<>();
		if (producers.errors().values().stream().anyMatch(KafkaErrors::isUnsupported)) {
			notes.add(UNSUPPORTED_PRODUCERS);
		}
		if (open.errors().values().stream().anyMatch(KafkaErrors::isUnsupported)) {
			notes.add(KafkaErrors.UNSUPPORTED + ": listTransactions requires Kafka 3.0+ brokers (the open transaction list is incomplete)");
		}
		if (!notes.isEmpty()) {
			out.put("notes", notes);
		}
		if (hanging > 0) {
			out.put("hint", "hangingSuspect: the transaction on the partition has been open longer than "
					+ "transaction.max.timeout.ms and the coordinator does not know it; it may be hanging "
					+ "(verify with kafka-transactions.sh find-hanging)");
		}
		if (!described.errors().isEmpty() || !producers.errors().isEmpty() || !open.errors().isEmpty()) {
			out.put("errors", Results.map("transactions", described.errors().isEmpty() ? null : described.errors(),
					"partitions", producers.errors().isEmpty() ? null : producers.errors().size(), "coordinators",
					open.errors().isEmpty() ? null : open.errors()));
		}
		return out;
	}

	private static String text(JsonNode n) {
		return n.isMissingNode() || n.isNull() ? null : n.asText();
	}

}

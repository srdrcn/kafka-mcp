package io.github.srdrcn.kafkamcp.toolsets.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.kafka.ClusterMeta;
import io.github.srdrcn.kafkamcp.kafka.Configs;
import io.github.srdrcn.kafkamcp.kafka.Format;
import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.kafka.LogDirs;
import io.github.srdrcn.kafkamcp.kafka.Topics;
import io.github.srdrcn.kafkamcp.mcp.P;
import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolSpec;

/** Broker okuma araçları: liste, config, config farkları, log dizinleri, partition dağılımı. */
@Component
public class BrokerTools {

	/**
	 * Broker'a özgü olduğu için karşılaştırmaya alınmayan anahtarlar. process.roles KRaft'ta düğüm rolüdür
	 * (broker / broker,controller); farklı olması drift değildir.
	 */
	static final List<String> PER_BROKER_KEYS = List.of("broker.id", "node.id", "broker.rack", "listeners",
			"advertised.listeners", "advertised.host.name", "advertised.port", "host.name", "port", "log.dir",
			"log.dirs", "metadata.log.dir", "process.roles");

	/**
	 * Sadece bir moda anlam taşıyan anahtarlar (önek ya da tam ad). Aynı cluster'da ZooKeeper ve KRaft
	 * broker'ları birlikteyse (ZooKeeper→KRaft geçişi) bunların farklı olması beklenir; karşılaştırılmaz.
	 */
	static final List<String> MODE_SPECIFIC_KEYS = List.of("zookeeper.", "controller.quorum.",
			"controller.listener.names", "inter.broker.protocol.version", "log.message.format.version",
			"broker.id.generation.enable", "reserved.broker.max.id", "process.roles");

	static final String AUTHORIZER = "authorizer.class.name";

	/** ZooKeeper broker'larının ACL authorizer'ları (AclAuthorizer ve öncülü SimpleAclAuthorizer). */
	static final Set<String> ZK_AUTHORIZERS = Set.of("kafka.security.authorizer.AclAuthorizer",
			"kafka.security.auth.SimpleAclAuthorizer");

	/** KRaft broker'larının ACL authorizer'ı. */
	static final String KRAFT_AUTHORIZER = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";

	public List<ToolSpec> tools() {
		return List.of(
				ToolSpec.read("brokers_list", "Broker'ları listeler: host, port, rack, controller mı, lider ve replika sayıları")
					.listParams()
					.handler(this::brokersList),
				ToolSpec.read("brokers_config_get", "Bir broker'ın config'leri (varsayılan: sadece varsayılandan farklı olanlar)")
					.param("brokerId", P.integer("Broker id (boşsa controller ya da ilk broker)"))
					.param("includeDefaults", P.bool("Varsayılan değerleri de getir (varsayılan false)"))
					.listParams()
					.handler(this::brokersConfigGet),
				ToolSpec.read("brokers_config_drift",
						"Broker'lar arasında değeri farklı olan config'ler (broker'a özgü anahtarlar; ZooKeeper ve KRaft broker'ları karışıksa moda özgü anahtarlar hariç)")
					.listParams()
					.handler(this::brokersConfigDrift),
				ToolSpec.read("brokers_logdirs",
						"Broker log dizinleri: yol, disk/kullanılan boyut, hata, partition sayısı ve en büyük partition'lar")
					.param("brokerId", P.integer("Sadece bu broker (boşsa hepsi)"))
					.listParams(20)
					.handler(this::brokersLogdirs),
				ToolSpec.read("brokers_partition_skew",
						"Broker başına lider/replika sayısı, ortalamadan sapma (%) ve tercih edilmeyen lider sayısı")
					.handler(this::brokersPartitionSkew));
	}

	Object brokersList(ToolCall call) {
		ClusterMeta.Info info = ClusterMeta.describe(call, true);
		Map<String, TopicDescription> topics = Topics.describeAll(call, true);
		Map<Integer, int[]> counts = new HashMap<>();
		for (TopicDescription d : topics.values()) {
			for (TopicPartitionInfo p : d.partitions()) {
				Integer leader = Topics.leader(p);
				if (leader != null) {
					counts.computeIfAbsent(leader, k -> new int[2])[0]++;
				}
				for (Node r : p.replicas()) {
					counts.computeIfAbsent(r.id(), k -> new int[2])[1]++;
				}
			}
		}
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Node n : info.brokers()) {
			if (!call.matches(n.host()) && !call.matches(String.valueOf(n.id()))) {
				continue;
			}
			int[] c = counts.getOrDefault(n.id(), new int[2]);
			Map<String, Object> row = ClusterMeta.node(n);
			row.put("isController", Objects.equals(info.controllerId(), n.id()));
			row.put("leaders", c[0]);
			row.put("replicas", c[1]);
			rows.add(row);
		}
		Map<String, Object> out = Results.map("controllerId", info.controllerId(), "mode",
				info.clusterMode().label(), "controllerNote", ClusterTools.controllerNote(info), "zkMigration",
				info.migration() ? true : null, "modeNote", info.migration() ? ClusterTools.MIGRATION_MODE_NOTE : null);
		out.putAll(Results.page("brokers", rows, call.limit()));
		return out;
	}

	Object brokersConfigGet(ToolCall call) {
		ClusterMeta.Info info = ClusterMeta.describe(call, !call.has("brokerId"));
		int id = call.has("brokerId") ? info.broker(call.optInt("brokerId")).id() : info.defaultBrokerId();
		boolean includeDefaults = call.bool("includeDefaults", false);
		Config config = Configs.describeOne(call, Configs.broker(id));
		List<Map<String, Object>> rows = new ArrayList<>();
		for (ConfigEntry e : Configs.sorted(config)) {
			if ((includeDefaults || !Configs.isDefault(e)) && call.matches(e.name())) {
				rows.add(Configs.row(e, false));
			}
		}
		Map<String, Object> out = Results.map("brokerId", id, "includeDefaults", includeDefaults);
		out.putAll(Results.page("configs", rows, call.limit()));
		return out;
	}

	Object brokersConfigDrift(ToolCall call) {
		ClusterMeta.Info info = ClusterMeta.describe(call, false);
		List<ConfigResource> resources = info.brokerIds().stream().map(Configs::broker).toList();
		Futures.Each<ConfigResource, Config> configs = Configs.describe(call, resources).requireAny();
		Map<Integer, Config> byBroker = byBrokerId(configs.values());
		Map<String, List<Integer>> modes = brokerModes(byBroker);
		boolean mixedModes = modes.size() > 1;
		boolean authorizerByMode = mixedModes && authorizerMatchesModes(byBroker);
		Drift drift = drift(byBroker, compared(byBroker, call::matches));
		Map<String, Object> out = Results.map("brokersCompared", List.copyOf(byBroker.keySet()), "excludedKeys",
				PER_BROKER_KEYS);
		if (mixedModes) {
			out.put("brokerModes", modes);
			out.put("modeNote", "Broker'lar farklı modda (ZooKeeper→KRaft geçişi?): moda özgü anahtarlar ("
					+ String.join(", ", MODE_SPECIFIC_KEYS) + ") karşılaştırılmadı"
					+ (authorizerByMode
							? "; authorizer.class.name her broker'da moduna uygun (ZooKeeper: AclAuthorizer, KRaft: StandardAuthorizer)"
							: ""));
		}
		if (!drift.presenceOnly().isEmpty()) {
			out.put("keysOnlyOnSomeBrokers", drift.presenceOnly().size());
			out.put("keysOnlyOnSomeBrokersNote",
					"Bazı broker'larda hiç olmayan, diğerlerinde varsayılan değerli anahtarlar (farklı Kafka sürümü/modu) fark sayılmadı");
		}
		if (!configs.errors().isEmpty()) {
			out.put("failedBrokers", byBrokerId(configs.errors()));
		}
		if (byBroker.size() < 2) {
			out.put("note", "Karşılaştırma için en az 2 broker gerekli");
		}
		out.putAll(Results.page("differences", drift.differences(), call.limit()));
		return out;
	}

	/**
	 * Karşılaştırılacak anahtarlar: broker'a özgü olmayan ve {@code matches}'e uyan; broker'lar farklı moddaysa
	 * moda özgü anahtarlar ve (her broker kendi modunun authorizer'ını kullanıyorsa) authorizer.class.name hariç.
	 */
	static Predicate<String> compared(Map<Integer, Config> byBroker, Predicate<String> matches) {
		boolean mixedModes = brokerModes(byBroker).size() > 1;
		boolean authorizerByMode = mixedModes && authorizerMatchesModes(byBroker);
		return name -> !PER_BROKER_KEYS.contains(name) && matches.test(name) && !(mixedModes && modeSpecific(name))
				&& !(authorizerByMode && AUTHORIZER.equals(name));
	}

	/**
	 * Geçişte beklenen authorizer farkı mı: her ZooKeeper broker'ı AclAuthorizer (ya da SimpleAclAuthorizer), her
	 * KRaft broker'ı StandardAuthorizer kullanıyor. Modu belirlenemeyen broker ya da başka bir kombinasyon (ör.
	 * bir tarafta authorizer yok) gerçek fark sayılır.
	 */
	static boolean authorizerMatchesModes(Map<Integer, Config> byBroker) {
		for (Config c : byBroker.values()) {
			ClusterMeta.Mode mode = brokerMode(c);
			String authorizer = Configs.value(c.get(AUTHORIZER));
			// Set.of null kabul etmez: authorizer yoksa (ACL kapalı) beklenen kombinasyon değil
			boolean expected = authorizer != null && (mode.isZooKeeper() ? ZK_AUTHORIZERS.contains(authorizer)
					: mode.isKraft() && KRAFT_AUTHORIZER.equals(authorizer));
			if (!expected) {
				return false;
			}
		}
		return true;
	}

	/** Anahtar moda özgü mü ({@link #MODE_SPECIFIC_KEYS})? */
	static boolean modeSpecific(String name) {
		for (String k : MODE_SPECIFIC_KEYS) {
			if (k.endsWith(".") ? name.startsWith(k) : name.equals(k)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Broker'ın kendi config'ine göre modu: process.roles doluysa KRaft, zookeeper.connect doluysa ZooKeeper,
	 * ikisi de yoksa bilinmiyor.
	 */
	static ClusterMeta.Mode brokerMode(Config c) {
		ConfigEntry roles = c.get("process.roles");
		if (roles != null && roles.value() != null && !roles.value().isBlank()) {
			return ClusterMeta.Mode.KRAFT;
		}
		ConfigEntry zk = c.get("zookeeper.connect");
		if (zk != null && zk.value() != null && !zk.value().isBlank()) {
			return ClusterMeta.Mode.ZOOKEEPER;
		}
		return ClusterMeta.Mode.UNKNOWN;
	}

	/** Mod → broker id'leri (modu belirlenemeyenler hariç). */
	static Map<String, List<Integer>> brokerModes(Map<Integer, Config> byBroker) {
		Map<String, List<Integer>> out = new TreeMap<>();
		byBroker.forEach((id, c) -> {
			ClusterMeta.Mode m = brokerMode(c);
			if (m.isKnown()) {
				out.computeIfAbsent(m.label(), k -> new ArrayList<>()).add(id);
			}
		});
		return out;
	}

	/** BROKER config kaynağı anahtarlı map'i broker id anahtarlı (sıralı) map'e çevirir. */
	static <V> Map<Integer, V> byBrokerId(Map<ConfigResource, V> byResource) {
		Map<Integer, V> out = new TreeMap<>();
		byResource.forEach((r, v) -> out.put(Integer.parseInt(r.name()), v));
		return out;
	}

	/** {@link #drift} sonucu: farklar ve sadece varlık farkı olan (sürüm/mod kaynaklı) anahtarlar. */
	record Drift(List<Map<String, Object>> differences, List<String> presenceOnly) {
	}

	/**
	 * Broker'lar arasında değeri farklı olan anahtarlar; her biri için (değer, kaynak) grupları ve o grubu
	 * paylaşan broker'lar (kalabalık grup önce). Anahtarı hiç döndürmeyen broker "(yok)" değeriyle sayılır.
	 */
	static List<Map<String, Object>> differences(Map<Integer, Config> byBroker, Predicate<String> include) {
		return drift(byBroker, include).differences();
	}

	/**
	 * {@link #differences} gibi; ancak anahtarı döndüren tüm broker'larda varsayılan değerdeyse ve sadece bazı
	 * broker'larda hiç yoksa (farklı Kafka sürümü ya da mod: ör. ZooKeeper 3.x'te olup KRaft 4.x'te olmayan
	 * anahtarlar) fark sayılmaz, {@link Drift#presenceOnly()} içinde döner.
	 */
	static Drift drift(Map<Integer, Config> byBroker, Predicate<String> include) {
		Map<String, Map<Integer, ConfigEntry>> byKey = new TreeMap<>();
		byBroker.forEach((broker, config) -> {
			for (ConfigEntry c : config.entries()) {
				if (include.test(c.name())) {
					byKey.computeIfAbsent(c.name(), k -> new TreeMap<>()).put(broker, c);
				}
			}
		});
		List<Map<String, Object>> diffs = new ArrayList<>();
		List<String> presenceOnly = new ArrayList<>();
		for (Map.Entry<String, Map<Integer, ConfigEntry>> e : byKey.entrySet()) {
			Map<List<String>, List<Integer>> groups = new LinkedHashMap<>();
			for (Integer broker : byBroker.keySet()) {
				ConfigEntry c = e.getValue().get(broker);
				groups.computeIfAbsent(Arrays.asList(Configs.value(c), Configs.source(c)), k -> new ArrayList<>())
					.add(broker);
			}
			if (groups.keySet().stream().map(k -> k.get(0)).distinct().count() < 2) {
				continue;
			}
			boolean missingSomewhere = e.getValue().size() < byBroker.size();
			boolean allDefault = e.getValue().values().stream().allMatch(Configs::isDefault);
			if (missingSomewhere && allDefault && distinctPresentValues(e.getValue()) < 2) {
				presenceOnly.add(e.getKey());
				continue;
			}
			List<Map<String, Object>> values = new ArrayList<>();
			groups.forEach((k, brokers) -> values.add(Results.map("value", k.get(0) == null ? "(yok)" : k.get(0),
					"source", k.get(1), "brokers", brokers)));
			values.sort(Comparator.comparingInt((Map<String, Object> m) -> -((List<?>) m.get("brokers")).size()));
			diffs.add(Results.map("name", e.getKey(), "values", values));
		}
		return new Drift(diffs, presenceOnly);
	}

	private static long distinctPresentValues(Map<Integer, ConfigEntry> entries) {
		return entries.values().stream().map(Configs::value).distinct().count();
	}

	Object brokersLogdirs(ToolCall call) {
		ClusterMeta.Info info = ClusterMeta.describe(call, false);
		List<Integer> ids = call.has("brokerId") ? List.of(info.broker(call.optInt("brokerId")).id())
				: info.brokerIds();
		Futures.Each<Integer, Map<String, LogDirDescription>> dirs = LogDirs.describe(call, ids, info.brokerIds())
			.requireAny();
		List<Map<String, Object>> brokers = new ArrayList<>();
		List<Replica> replicas = new ArrayList<>();
		for (Integer id : ids) {
			Map<String, LogDirDescription> byPath = dirs.values().get(id);
			if (byPath == null) {
				brokers.add(Results.map("brokerId", id, "error", dirs.errors().get(id)));
				continue;
			}
			List<Map<String, Object>> dirRows = new ArrayList<>();
			for (Map.Entry<String, LogDirDescription> d : new TreeMap<>(byPath).entrySet()) {
				LogDirDescription desc = d.getValue();
				long size = 0;
				int future = 0;
				for (Map.Entry<TopicPartition, ReplicaInfo> r : desc.replicaInfos().entrySet()) {
					size += r.getValue().size();
					if (r.getValue().isFuture()) {
						future++;
					}
					if (call.matches(r.getKey().topic())) {
						replicas.add(new Replica(r.getKey(), id, d.getKey(), r.getValue()));
					}
				}
				Map<String, Object> row = Results.map("path", d.getKey(), "error",
						desc.error() == null ? null : KafkaErrors.explain(desc.error()), "partitions",
						desc.replicaInfos().size(), "sizeBytes", size, "size", Format.bytes(size), "futureReplicas",
						future > 0 ? future : null);
				if (desc.totalBytes().isEmpty() || desc.usableBytes().isEmpty()) {
					if (desc.error() == null) {
						// DescribeLogDirs v4 (Kafka 3.3+) öncesi broker'lar disk boyutunu döndürmez
						row.put("disk", KafkaErrors.UNSUPPORTED + ": disk boyutu Kafka 3.3+ broker'larda var");
					}
				}
				else {
					long total = desc.totalBytes().getAsLong();
					long usable = desc.usableBytes().getAsLong();
					row.put("totalBytes", total);
					row.put("usableBytes", usable);
					row.put("disk", Format.bytes(usable) + " boş / " + Format.bytes(total));
					if (total > 0) {
						row.put("diskUsedPct", Format.round(100.0 * (total - usable) / total, 1));
					}
				}
				dirRows.add(row);
			}
			brokers.add(Results.map("brokerId", id, "dirs", dirRows));
		}
		replicas.sort(Comparator.comparingLong((Replica r) -> r.info().size()).reversed());
		List<Map<String, Object>> largest = new ArrayList<>();
		for (Replica r : replicas.subList(0, Math.min(call.limit(20), replicas.size()))) {
			largest.add(Results.map("topic", r.tp().topic(), "partition", r.tp().partition(), "brokerId", r.broker(),
					"dir", r.dir(), "sizeBytes", r.info().size(), "size", Format.bytes(r.info().size()), "future",
					r.info().isFuture() ? true : null));
		}
		Map<String, Object> out = Results.map("brokers", brokers);
		out.putAll(Params.page("largestPartitions", largest, replicas.size()));
		return out;
	}

	/** Sıralama için hafif replika kaydı (map'e sadece ilk limit kadarı çevrilir). */
	private record Replica(TopicPartition tp, int broker, String dir, ReplicaInfo info) {
	}

	Object brokersPartitionSkew(ToolCall call) {
		ClusterMeta.Info info = ClusterMeta.describe(call, false);
		Map<String, TopicDescription> topics = Topics.describeAll(call, true);
		// [lider, replika, tercih edilen lider olduğu, lider ama tercih edilen değil]
		Map<Integer, int[]> counts = new TreeMap<>();
		info.brokerIds().forEach(id -> counts.put(id, new int[4]));
		int partitions = 0;
		int offline = 0;
		int nonPreferred = 0;
		long replicaTotal = 0;
		for (TopicDescription d : topics.values()) {
			for (TopicPartitionInfo p : d.partitions()) {
				partitions++;
				Integer leader = Topics.leader(p);
				if (leader == null) {
					offline++;
				}
				else {
					counts.computeIfAbsent(leader, k -> new int[4])[0]++;
				}
				for (Node r : p.replicas()) {
					counts.computeIfAbsent(r.id(), k -> new int[4])[1]++;
					replicaTotal++;
				}
				Integer preferred = Topics.preferredLeader(p);
				if (preferred != null) {
					counts.computeIfAbsent(preferred, k -> new int[4])[2]++;
				}
				if (Topics.nonPreferredLeader(p)) {
					nonPreferred++;
					counts.get(leader)[3]++;
				}
			}
		}
		int live = Math.max(1, info.brokers().size());
		double avgLeaders = (double) (partitions - offline) / live;
		double avgReplicas = (double) replicaTotal / live;
		Set<Integer> liveIds = Set.copyOf(info.brokerIds());
		double maxLeaderDev = 0;
		double maxReplicaDev = 0;
		List<Map<String, Object>> rows = new ArrayList<>();
		for (Map.Entry<Integer, int[]> e : counts.entrySet()) {
			int[] c = e.getValue();
			boolean online = liveIds.contains(e.getKey());
			double leaderDev = online ? deviation(c[0], avgLeaders) : 0;
			double replicaDev = online ? deviation(c[1], avgReplicas) : 0;
			maxLeaderDev = Math.max(maxLeaderDev, Math.abs(leaderDev));
			maxReplicaDev = Math.max(maxReplicaDev, Math.abs(replicaDev));
			rows.add(Results.map("brokerId", e.getKey(), "online", online ? null : false, "leaders", c[0],
					"replicas", c[1], "leaderDeviationPct", leaderDev, "replicaDeviationPct", replicaDev,
					"preferredLeaderOf", c[2], "nonPreferredLeaders", c[3]));
		}
		Map<String, Object> summary = Results.map("brokers", info.brokers().size(), "partitions", partitions,
				"replicas", replicaTotal, "offlinePartitions", offline, "nonPreferredLeaderPartitions", nonPreferred,
				"avgLeadersPerBroker", Format.round(avgLeaders, 1), "avgReplicasPerBroker", Format.round(avgReplicas, 1),
				"maxLeaderDeviationPct", Format.round(maxLeaderDev, 1), "maxReplicaDeviationPct",
				Format.round(maxReplicaDev, 1), "skewed", maxLeaderDev > 20 || maxReplicaDev > 20);
		Map<String, Object> out = Results.map("summary", summary, "brokers", rows);
		if (nonPreferred > 0) {
			out.put("hint", "Tercih edilen lidere dönmek için brokers_elect_preferred_leaders kullanılabilir");
		}
		if (rows.stream().anyMatch(r -> Boolean.FALSE.equals(r.get("online")))) {
			out.put("note", "online=false: replikası olan ama erişilebilir olmayan broker");
		}
		return out;
	}

	private static double deviation(int value, double avg) {
		return avg == 0 ? 0 : Format.round((value - avg) / avg * 100, 1);
	}

}

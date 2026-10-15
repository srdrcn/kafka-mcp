package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.FeatureMetadata;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Cluster metadata'sı: broker'lar, aktif controller, cluster modu (KRaft/ZooKeeper) ve (KRaft'ta) metadata
 * quorum'u.
 *
 * <p>
 * Mod tespiti: describeMetadataQuorum başarılıysa KRaft. Değilse (yetki yok, zaman aşımı, desteklenmiyor)
 * describeFeatures'a bakılır: finalize edilmiş {@code metadata.version} varsa KRaft, finalize edilmiş feature
 * yoksa ZooKeeper (ZooKeeper broker'ları metadata.version finalize etmez). describeFeatures ApiVersions
 * üzerinden gider ve yetki istemez; ikisi de yanıtsızsa mod {@link Mode#UNKNOWN}.
 *
 * <p>
 * Aktif controller: KRaft'ta quorum lideri (broker'lar describeCluster'da rastgele bir canlı broker'ı
 * "controller" diye döndürür; client'lar controller'lara erişemez), ZooKeeper'da describeCluster controller'ı.
 * ZooKeeper→KRaft geçişinde ({@value #MIGRATION_ENABLE}=true) KRaft controller'ı devraldıktan sonra ZooKeeper
 * broker'ları da rastgele broker döndürür; bu yüzden ZooKeeper modunda controller broker'ının bu bayrağına bakılır
 * ve geçiş açıksa controller raporlanmaz ({@link Info#zkMigration()}). Geçiş sırasında (bazı broker'lar KRaft)
 * mod, isteği yanıtlayan broker'a göre değişebilir.
 */
public final class ClusterMeta {

	/** {@link Info#mode()} değeri: KRaft. */
	public static final String KRAFT = "KRaft";

	/** {@link Info#mode()} değeri: ZooKeeper (describeMetadataQuorum desteklenmiyor). */
	public static final String ZOOKEEPER = "ZooKeeper";

	/** Mod belirlenemediğinde çıktıda kullanılan değer ({@link Mode#UNKNOWN}). */
	public static final String UNKNOWN = "unknown";

	/** KRaft'ın finalize ettiği metadata sürümü feature'ı; ZooKeeper cluster'larında yoktur. */
	public static final String METADATA_VERSION_FEATURE = "metadata.version";

	/**
	 * describeMetadataQuorum için en fazla bekleme. Quorum lideri yokken istek deadline'a kadar asılı kalır;
	 * broker'lar topic/config sorgularına cache'ten yanıt verebildiği için kalan süre onlara bırakılır.
	 */
	static final long QUORUM_WAIT_MS = 4_000;

	/** describeFeatures (ApiVersions) için en fazla bekleme. */
	static final long FEATURES_WAIT_MS = 4_000;

	/** ZooKeeper→KRaft geçiş bayrağı (broker config'i); ZooKeeper ve geçişteki KRaft broker'larında true. */
	public static final String MIGRATION_ENABLE = "zookeeper.metadata.migration.enable";

	/** ZooKeeper modunda geçiş bayrağı için controller broker'ının config'ini en fazla bekleme. */
	static final long MIGRATION_WAIT_MS = 4_000;

	private ClusterMeta() {
	}

	/** Cluster modu. */
	public enum Mode {

		/** KRaft (metadata quorum'u ya da finalize edilmiş metadata.version var). */
		KRAFT(ClusterMeta.KRAFT),

		/** ZooKeeper (DescribeQuorum desteklenmiyor / finalize edilmiş feature yok). */
		ZOOKEEPER(ClusterMeta.ZOOKEEPER),

		/** Belirlenemedi (quorum ve feature sorguları başarısız). */
		UNKNOWN(ClusterMeta.UNKNOWN);

		private final String label;

		Mode(String label) {
			this.label = label;
		}

		/** Çıktıda kullanılan ad: "KRaft", "ZooKeeper" ya da "unknown". */
		public String label() {
			return label;
		}

		public boolean isKraft() {
			return this == KRAFT;
		}

		public boolean isZooKeeper() {
			return this == ZOOKEEPER;
		}

		public boolean isKnown() {
			return this != UNKNOWN;
		}

		/** {@link Info#mode()} değerinden (null = bilinmiyor). */
		public static Mode of(String label) {
			if (ClusterMeta.KRAFT.equals(label)) {
				return KRAFT;
			}
			if (ClusterMeta.ZOOKEEPER.equals(label)) {
				return ZOOKEEPER;
			}
			return UNKNOWN;
		}

		@Override
		public String toString() {
			return label;
		}

	}

	/**
	 * describeCluster sonucu. {@code controllerId} KRaft'ta quorum lideri, ZooKeeper'da describeCluster
	 * controller'ıdır; quorum sorgulanmadıysa, KRaft'ta quorum sorgusu başarısızsa ya da ZooKeeper→KRaft geçişi
	 * açıksa null (bu durumlarda broker'lar describeCluster'da rastgele bir broker'ı controller diye döndürür).
	 * {@code mode} quorum sorgulanmadıysa ya da belirlenemediyse null. {@code finalizedFeatures} finalize edilmiş
	 * feature'lar (ad → en yüksek seviye; ZooKeeper'da boş), sorgulanmadıysa ya da sorgu başarısızsa null.
	 * {@code zkMigration} ZooKeeper modunda controller broker'ının {@value #MIGRATION_ENABLE} değeri; KRaft'ta,
	 * sorgulanmadıysa ya da config okunamadıysa null.
	 */
	public record Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
			String quorumError, Map<String, Short> finalizedFeatures, Boolean zkMigration) {

		/** Geçiş bilgisi olmadan (geriye uyumluluk). */
		public Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
				String quorumError, Map<String, Short> finalizedFeatures) {
			this(clusterId, brokers, controllerId, mode, quorum, quorumError, finalizedFeatures, null);
		}

		/** Feature bilgisi olmadan (geriye uyumluluk). */
		public Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
				String quorumError) {
			this(clusterId, brokers, controllerId, mode, quorum, quorumError, null, null);
		}

		/**
		 * ZooKeeper→KRaft geçişi açık mı? true ise describeCluster controller'ı güvenilmez (KRaft controller'ı
		 * devraldıysa ZooKeeper broker'ları rastgele broker döndürür) ve {@link #controllerId()} null'dır.
		 */
		public boolean migration() {
			return Boolean.TRUE.equals(zkMigration);
		}

		/** Broker id'leri (sıralı). */
		public List<Integer> brokerIds() {
			return brokers.stream().map(Node::id).toList();
		}

		/** Varsayılan broker: controller broker ise o, değilse en küçük id. */
		public int defaultBrokerId() {
			if (brokers.isEmpty()) {
				throw new ToolException("Cluster'da erişilebilir broker yok");
			}
			if (controllerIsBroker()) {
				return controllerId;
			}
			return brokers.get(0).id();
		}

		/** Broker'ı id ile bulur; yoksa Türkçe hata. */
		public Node broker(int id) {
			return brokers.stream()
				.filter(n -> n.id() == id)
				.findFirst()
				.orElseThrow(() -> new ToolException("Broker bulunamadı: " + id + " (mevcut: " + brokerIds() + ")"));
		}

		/** Mod; {@link #mode()} null ise {@link Mode#UNKNOWN}. */
		public Mode clusterMode() {
			return Mode.of(mode);
		}

		/** Aktif controller aynı zamanda erişilebilir bir broker mı (ZooKeeper ya da KRaft combined mod)? */
		public boolean controllerIsBroker() {
			return controllerId != null && brokerIds().contains(controllerId);
		}

		/** Finalize edilmiş metadata.version seviyesi (sadece KRaft) ya da null. */
		public Short metadataVersionLevel() {
			return finalizedFeatures == null ? null : finalizedFeatures.get(METADATA_VERSION_FEATURE);
		}

	}

	/**
	 * describeCluster (+ withQuorum ise describeMetadataQuorum ve describeFeatures) çağrılarını paralel gönderir.
	 * Quorum ve feature'lar en fazla birkaç saniye beklenir; yanıt gelmezse {@link Info#quorumError()} dolar ve
	 * araç devam eder. withQuorum false ise sadece describeCluster (mod ve controller null).
	 */
	public static Info describe(ToolCall call, boolean withQuorum) {
		Admin admin = call.admin();
		DescribeClusterResult dc = admin.describeCluster();
		KafkaFuture<QuorumInfo> qf = withQuorum ? admin.describeMetadataQuorum().quorumInfo() : null;
		KafkaFuture<FeatureMetadata> ff = withQuorum ? admin.describeFeatures().featureMetadata() : null;
		// iki istek birlikte gönderildi: beklemeler gönderimden itibaren sayılır (toplam en fazla ~4 sn)
		long quorumEnd = System.currentTimeMillis() + QUORUM_WAIT_MS;
		long featuresEnd = System.currentTimeMillis() + FEATURES_WAIT_MS;
		String clusterId = call.await(dc.clusterId());
		List<Node> nodes = new ArrayList<>(call.await(dc.nodes()));
		nodes.sort(Comparator.comparingInt(Node::id));
		Node controller = call.await(dc.controller());
		if (!withQuorum) {
			return new Info(clusterId, List.copyOf(nodes), null, null, null, null, null, null);
		}
		QuorumInfo quorum = null;
		String quorumError = null;
		boolean quorumUnsupported = false;
		try {
			quorum = Futures.awaitAtMost(call, qf, untilMillis(quorumEnd));
		}
		catch (ToolException e) {
			quorumUnsupported = KafkaErrors.isUnsupported(e);
			quorumError = e.getMessage();
		}
		Map<String, Short> features = finalized(call, ff, untilMillis(featuresEnd));
		Mode mode = resolveMode(quorum != null, quorumUnsupported, features);
		if (mode == Mode.ZOOKEEPER) {
			// ZooKeeper'da DescribeQuorum zaten yok: hata değil
			quorumError = null;
		}
		Integer controllerId = null;
		Boolean migration = null;
		if (quorum != null) {
			controllerId = quorum.leaderId() >= 0 ? quorum.leaderId() : null;
		}
		else if (mode == Mode.ZOOKEEPER && controller != null && !controller.isEmpty()) {
			// geçişte KRaft controller'ı devraldıysa bu rastgele bir broker'dır: bayrak açıksa raporlanmaz
			migration = zkMigration(call, admin, controller.id(), MIGRATION_WAIT_MS);
			controllerId = Boolean.TRUE.equals(migration) ? null : controller.id();
		}
		return new Info(clusterId, List.copyOf(nodes), controllerId, mode.isKnown() ? mode.label() : null, quorum,
				quorumError, features, migration);
	}

	/**
	 * Broker config'inde ZooKeeper→KRaft geçişi açık mı ({@value #MIGRATION_ENABLE}=true)? Geçişte hem ZooKeeper
	 * hem KRaft'a taşınmış broker'larda açıktır (geçiş sonlandırılınca kaldırılır).
	 */
	public static boolean migrationEnabled(Config brokerConfig) {
		ConfigEntry e = brokerConfig == null ? null : brokerConfig.get(MIGRATION_ENABLE);
		return e != null && !e.isSensitive() && "true".equalsIgnoreCase(e.value());
	}

	/**
	 * Broker'ın {@value #MIGRATION_ENABLE} değeri (tek describeConfigs, en fazla {@code maxMillis}); config
	 * okunamazsa (yetki yok, zaman aşımı) null. Hata fırlatmaz.
	 */
	public static Boolean zkMigration(ToolCall call, Admin admin, int brokerId, long maxMillis) {
		try {
			ConfigResource r = Configs.broker(brokerId);
			int timeout = (int) Math.max(1, Math.min(maxMillis, call.waitMillis()));
			KafkaFuture<Config> f = admin.describeConfigs(List.of(r), new DescribeConfigsOptions().timeoutMs(timeout))
				.values()
				.get(r);
			return f == null ? null : migrationEnabled(Futures.awaitAtMost(call, f, maxMillis));
		}
		catch (RuntimeException e) {
			return null;
		}
	}

	/**
	 * Seçilen cluster'ın modu; araçlar (report/diagnose/acl...) moda göre davranmak için çağırır. Önce
	 * describeFeatures (yetki istemez, broker cache'ten yanıtlar), başarısızsa describeMetadataQuorum denenir.
	 * Her istek en fazla birkaç saniye beklenir; ikisi de yanıtsızsa {@link Mode#UNKNOWN}. Hata fırlatmaz.
	 * ZooKeeper→KRaft geçişinde (bazı broker'lar KRaft) sonuç yanıtlayan broker'ın modudur.
	 */
	public static Mode mode(ToolCall call) {
		Admin admin;
		try {
			admin = call.admin();
		}
		catch (ToolException e) {
			return Mode.UNKNOWN;
		}
		long end = System.currentTimeMillis() + QUORUM_WAIT_MS;
		Map<String, Short> features = finalized(call, admin.describeFeatures().featureMetadata(), FEATURES_WAIT_MS);
		if (features != null) {
			return modeOf(features);
		}
		// feature sorgusu hata verdiyse kalan sürede quorum denenir (zaman aşımıysa beklemeden UNKNOWN)
		try {
			Futures.awaitAtMost(call, admin.describeMetadataQuorum().quorumInfo(), untilMillis(end));
			return Mode.KRAFT;
		}
		catch (ToolException e) {
			return KafkaErrors.isUnsupported(e) ? Mode.ZOOKEEPER : Mode.UNKNOWN;
		}
	}

	/**
	 * Finalize edilmiş feature'lardan mod: metadata.version varsa KRaft, yoksa ZooKeeper; null ise
	 * {@link Mode#UNKNOWN}. Değer tipi önemsizdir (FinalizedVersionRange ya da seviye).
	 */
	public static Mode modeOf(Map<String, ?> finalizedFeatures) {
		if (finalizedFeatures == null) {
			return Mode.UNKNOWN;
		}
		return finalizedFeatures.containsKey(METADATA_VERSION_FEATURE) ? Mode.KRAFT : Mode.ZOOKEEPER;
	}

	/** describeFeatures sonucundan mod (bkz. {@link #modeOf(Map)}). */
	public static Mode modeOf(FeatureMetadata features) {
		return modeOf(features == null ? null : features.finalizedFeatures());
	}

	/** Finalize edilmiş feature'lar: ad → en yüksek seviye (ada göre sıralı). */
	public static Map<String, Short> levels(FeatureMetadata features) {
		Map<String, Short> out = new TreeMap<>();
		if (features != null) {
			for (Map.Entry<String, FinalizedVersionRange> e : features.finalizedFeatures().entrySet()) {
				out.put(e.getKey(), e.getValue().maxVersionLevel());
			}
		}
		return out;
	}

	/**
	 * Quorum ve feature sorgularının sonucundan mod. Quorum yanıtı KRaft demektir; yoksa feature'lar belirler
	 * (metadata.version → KRaft; eski KRaft broker'ları DescribeQuorum'u desteklemese de metadata.version'ı
	 * finalize eder). İkisi de yoksa "desteklenmiyor" hatası ZooKeeper demektir.
	 */
	static Mode resolveMode(boolean quorumOk, boolean quorumUnsupported, Map<String, ?> finalizedFeatures) {
		if (quorumOk) {
			return Mode.KRAFT;
		}
		if (finalizedFeatures != null) {
			return modeOf(finalizedFeatures);
		}
		return quorumUnsupported ? Mode.ZOOKEEPER : Mode.UNKNOWN;
	}

	/** Gönderilmiş describeFeatures'ı en fazla {@code maxMillis} bekler; hata/zaman aşımında null. */
	private static Map<String, Short> finalized(ToolCall call, KafkaFuture<FeatureMetadata> pending, long maxMillis) {
		try {
			return levels(Futures.awaitAtMost(call, pending, maxMillis));
		}
		catch (ToolException e) {
			return null;
		}
	}

	/** Verilen ana kadar kalan süre (en az 1 ms). */
	private static long untilMillis(long endEpochMillis) {
		return Math.max(1, endEpochMillis - System.currentTimeMillis());
	}

	/** Broker'ın kompakt gösterimi: id, host, port, rack. */
	public static Map<String, Object> node(Node n) {
		return Results.map("id", n.id(), "host", n.host(), "port", n.port(), "rack", n.hasRack() ? n.rack() : null);
	}

}

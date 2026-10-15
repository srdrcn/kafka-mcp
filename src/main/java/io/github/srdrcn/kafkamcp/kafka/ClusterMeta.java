package io.github.srdrcn.kafkamcp.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import io.github.srdrcn.kafkamcp.mcp.Results;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/** Cluster metadata'sı: broker'lar, aktif controller ve (KRaft'ta) metadata quorum'u. */
public final class ClusterMeta {

	/** {@link Info#mode()} değeri: KRaft. */
	public static final String KRAFT = "KRaft";

	/** {@link Info#mode()} değeri: ZooKeeper (describeMetadataQuorum desteklenmiyor). */
	public static final String ZOOKEEPER = "ZooKeeper";

	/**
	 * describeMetadataQuorum için en fazla bekleme. Quorum lideri yokken istek deadline'a kadar asılı kalır;
	 * broker'lar topic/config sorgularına cache'ten yanıt verebildiği için kalan süre onlara bırakılır.
	 */
	static final long QUORUM_WAIT_MS = 4_000;

	private ClusterMeta() {
	}

	/**
	 * describeCluster sonucu. {@code controllerId} KRaft'ta quorum lideri, ZooKeeper'da describeCluster
	 * controller'ıdır; quorum sorgulanmadıysa ya da sorgu başarısızsa null (KRaft broker'ları describeCluster'da
	 * rastgele bir broker'ı controller diye döndürür). {@code mode} quorum sorgulanmadıysa ya da
	 * belirlenemediyse null.
	 */
	public record Info(String clusterId, List<Node> brokers, Integer controllerId, String mode, QuorumInfo quorum,
			String quorumError) {

		/** Broker id'leri (sıralı). */
		public List<Integer> brokerIds() {
			return brokers.stream().map(Node::id).toList();
		}

		/** Varsayılan broker: controller broker ise o, değilse en küçük id. */
		public int defaultBrokerId() {
			if (brokers.isEmpty()) {
				throw new ToolException("Cluster'da erişilebilir broker yok");
			}
			if (controllerId != null && brokerIds().contains(controllerId)) {
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

	}

	/**
	 * describeCluster (+ withQuorum ise describeMetadataQuorum) çağrılarını paralel gönderir. Quorum en fazla
	 * {@link #QUORUM_WAIT_MS} beklenir; yanıt gelmezse {@link Info#quorumError()} dolar ve araç devam eder.
	 */
	public static Info describe(ToolCall call, boolean withQuorum) {
		Admin admin = call.admin();
		DescribeClusterResult dc = admin.describeCluster();
		KafkaFuture<QuorumInfo> qf = withQuorum ? admin.describeMetadataQuorum().quorumInfo() : null;
		String clusterId = call.await(dc.clusterId());
		List<Node> nodes = new ArrayList<>(call.await(dc.nodes()));
		nodes.sort(Comparator.comparingInt(Node::id));
		Node controller = call.await(dc.controller());
		QuorumInfo quorum = null;
		String mode = null;
		String quorumError = null;
		if (qf != null) {
			try {
				quorum = Futures.awaitAtMost(call, qf, QUORUM_WAIT_MS);
				mode = KRAFT;
			}
			catch (ToolException e) {
				if (KafkaErrors.unwrap(e.getCause()) instanceof UnsupportedVersionException) {
					mode = ZOOKEEPER;
				}
				else {
					quorumError = e.getMessage();
				}
			}
		}
		Integer controllerId = null;
		if (quorum != null) {
			controllerId = quorum.leaderId();
		}
		else if (ZOOKEEPER.equals(mode) && controller != null && !controller.isEmpty()) {
			controllerId = controller.id();
		}
		return new Info(clusterId, List.copyOf(nodes), controllerId, mode, quorum, quorumError);
	}

	/** Broker'ın kompakt gösterimi: id, host, port, rack. */
	public static Map<String, Object> node(Node n) {
		return Results.map("id", n.id(), "host", n.host(), "port", n.port(), "rack", n.hasRack() ? n.rack() : null);
	}

}

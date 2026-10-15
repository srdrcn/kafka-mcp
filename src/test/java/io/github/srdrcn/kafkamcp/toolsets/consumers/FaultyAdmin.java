package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * Gerçek Admin'i saran ve iki arıza senaryosunu üreten test proxy'si:
 * <ul>
 * <li>hangTopics: bu topic'lerin partition'ları listOffsets'te var olmayan partition numarasına çevrilir;
 * gerçek AdminClient "leader unknown, will retry" diye istek timeoutMs'i (yoksa default.api.timeout.ms)
 * dolana kadar dener: leader'ı olmayan partition'ın birebir davranışı.</li>
 * <li>hangGroups: listConsumerGroupOffsets/describeConsumerGroups'ta bu grupların future'ı, AdminClient'ın
 * COORDINATOR_LOAD_IN_PROGRESS'i yeniden denemesi gibi, istek timeoutMs'i (yoksa 60 sn) dolunca
 * TimeoutException ile biter.</li>
 * </ul>
 */
final class FaultyAdmin implements InvocationHandler {

	private static final int DEFAULT_API_TIMEOUT_MS = 60_000;

	private static final int FAKE_PARTITION_OFFSET = 100_000;

	private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
		Thread t = new Thread(r, "faulty-admin");
		t.setDaemon(true);
		return t;
	});

	private final Admin real;

	private final Set<String> hangGroups;

	private final Set<String> hangTopics;

	private FaultyAdmin(Admin real, Set<String> hangGroups, Set<String> hangTopics) {
		this.real = real;
		this.hangGroups = hangGroups;
		this.hangTopics = hangTopics;
	}

	static Admin wrap(Admin real, Set<String> hangGroups, Set<String> hangTopics) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				new FaultyAdmin(real, hangGroups, hangTopics));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String name = method.getName();
		Class<?>[] types = method.getParameterTypes();
		if (name.equals("listOffsets") && types.length >= 1 && types[0] == Map.class) {
			return listOffsets((Map<TopicPartition, OffsetSpec>) args[0],
					args.length > 1 ? (ListOffsetsOptions) args[1] : new ListOffsetsOptions());
		}
		if (name.equals("listConsumerGroupOffsets") && types.length >= 1 && types[0] == Map.class) {
			return listConsumerGroupOffsets((Map<String, ListConsumerGroupOffsetsSpec>) args[0],
					args.length > 1 ? (ListConsumerGroupOffsetsOptions) args[1] : new ListConsumerGroupOffsetsOptions());
		}
		if (name.equals("describeConsumerGroups") && types.length >= 1) {
			return describeConsumerGroups((Collection<String>) args[0],
					args.length > 1 ? (DescribeConsumerGroupsOptions) args[1] : new DescribeConsumerGroupsOptions());
		}
		try {
			return method.invoke(real, args);
		}
		catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	private ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> request, ListOffsetsOptions options) {
		Map<TopicPartition, TopicPartition> original = new HashMap<>();
		Map<TopicPartition, OffsetSpec> rewritten = new HashMap<>();
		request.forEach((tp, spec) -> {
			TopicPartition sent = hangTopics.contains(tp.topic())
					? new TopicPartition(tp.topic(), tp.partition() + FAKE_PARTITION_OFFSET) : tp;
			original.put(sent, tp);
			rewritten.put(sent, spec);
		});
		ListOffsetsResult r = real.listOffsets(rewritten, options);
		Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
		original.forEach((sent, tp) -> futures.put(tp, r.partitionResult(sent)));
		return new ListOffsetsResult(futures);
	}

	private ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> specs,
			ListConsumerGroupOffsetsOptions options) throws ReflectiveOperationException {
		Map<String, ListConsumerGroupOffsetsSpec> ok = new LinkedHashMap<>(specs);
		ok.keySet().removeAll(hangGroups);
		ListConsumerGroupOffsetsResult r = ok.isEmpty() ? null : real.listConsumerGroupOffsets(ok, options);
		Map<CoordinatorKey, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new HashMap<>();
		for (String g : specs.keySet()) {
			futures.put(CoordinatorKey.byGroupId(g),
					hangGroups.contains(g) ? hanging(options) : r.partitionsToOffsetAndMetadata(g));
		}
		Constructor<ListConsumerGroupOffsetsResult> c = ListConsumerGroupOffsetsResult.class
			.getDeclaredConstructor(Map.class);
		c.setAccessible(true);
		return c.newInstance(futures);
	}

	private DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groups,
			DescribeConsumerGroupsOptions options) {
		List<String> ok = new ArrayList<>(groups);
		ok.removeAll(hangGroups);
		Map<String, KafkaFuture<ConsumerGroupDescription>> real = ok.isEmpty() ? Map.of()
				: this.real.describeConsumerGroups(ok, options).describedGroups();
		Map<String, KafkaFuture<ConsumerGroupDescription>> futures = new HashMap<>();
		for (String g : groups) {
			futures.put(g, hangGroups.contains(g) ? hanging(options) : real.get(g));
		}
		return new DescribeConsumerGroupsResult(futures);
	}

	/** İsteğin timeoutMs'i dolunca TimeoutException ile biten future (AdminClient'ın yeniden deneme davranışı). */
	private static <T> KafkaFuture<T> hanging(AbstractOptions<?> options) {
		KafkaFutureImpl<T> f = new KafkaFutureImpl<>();
		int timeout = options.timeoutMs() == null ? DEFAULT_API_TIMEOUT_MS : options.timeoutMs();
		SCHEDULER.schedule(
				() -> f.completeExceptionally(new TimeoutException("simulated: coordinator load in progress")),
				timeout, TimeUnit.MILLISECONDS);
		return f;
	}

}

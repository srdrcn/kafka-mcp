package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

/**
 * Test proxy that wraps the real Admin and mimics the responses of old brokers ({@link Era}):
 * <ul>
 * <li>{@link Era#KAFKA_2_3}: no state and no type in listGroups (before ListGroups v4, Kafka &lt; 2.6); no
 * OffsetDelete API for deleteConsumerGroupOffsets (Kafka &lt; 2.4), an UnsupportedVersionException as the
 * AdminClient gives it.</li>
 * <li>{@link Era#KAFKA_3_7}: state but no type in listGroups (ListGroups v4, Kafka 2.6-3.7; most real
 * ZooKeeper fleets).</li>
 * </ul>
 * Group ids sent to describeConsumerGroups are collected in {@link #describedGroups()}.
 */
final class OldBrokerAdmin implements InvocationHandler {

	enum Era {

		/** ListGroups v3 (no state/type), no OffsetDelete. */
		KAFKA_2_3,

		/** ListGroups v4 (state, no type). */
		KAFKA_3_7

	}

	private final Admin real;

	private final Era era;

	private final Admin proxy;

	private final List<String> described = new CopyOnWriteArrayList<>();

	private OldBrokerAdmin(Admin real, Era era) {
		this.real = real;
		this.era = era;
		this.proxy = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				this);
	}

	/** Kafka 2.3 broker ({@link Era#KAFKA_2_3}). */
	static Admin wrap(Admin real) {
		return of(real, Era.KAFKA_2_3).admin();
	}

	static OldBrokerAdmin of(Admin real, Era era) {
		return new OldBrokerAdmin(real, era);
	}

	Admin admin() {
		return proxy;
	}

	/** Group ids sent to describeConsumerGroups (in call order, including repeats). */
	List<String> describedGroups() {
		return List.copyOf(described);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String name = method.getName();
		if (name.equals("listGroups")) {
			return listGroups(args == null || args.length == 0 ? new ListGroupsOptions() : (ListGroupsOptions) args[0]);
		}
		if (name.equals("describeConsumerGroups") && args != null && args.length >= 1) {
			described.addAll((Collection<String>) args[0]);
		}
		if (name.equals("deleteConsumerGroupOffsets") && era == Era.KAFKA_2_3) {
			KafkaFutureImpl<Map<TopicPartition, Errors>> f = new KafkaFutureImpl<>();
			f.completeExceptionally(new UnsupportedVersionException(
					"The broker does not support OFFSET_DELETE (simulated Kafka 2.3 broker)"));
			Constructor<DeleteConsumerGroupOffsetsResult> c = DeleteConsumerGroupOffsetsResult.class
				.getDeclaredConstructor(KafkaFuture.class, Set.class);
			c.setAccessible(true);
			return c.newInstance(f, (Set<TopicPartition>) args[1]);
		}
		try {
			return method.invoke(real, args);
		}
		catch (InvocationTargetException e) {
			throw e.getCause();
		}
	}

	private ListGroupsResult listGroups(ListGroupsOptions options) throws ReflectiveOperationException {
		KafkaFutureImpl<Collection<Object>> stripped = new KafkaFutureImpl<>();
		real.listGroups(options).valid().whenComplete((groups, error) -> {
			if (error != null) {
				stripped.completeExceptionally(error);
				return;
			}
			stripped.complete(groups.stream()
				.map(g -> (Object) new GroupListing(g.groupId(), Optional.empty(), g.protocol(),
						era == Era.KAFKA_3_7 ? g.groupState() : Optional.empty()))
				.toList());
		});
		Constructor<ListGroupsResult> c = ListGroupsResult.class.getDeclaredConstructor(KafkaFuture.class);
		c.setAccessible(true);
		return c.newInstance(stripped);
	}

}

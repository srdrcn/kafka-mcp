package io.github.srdrcn.kafkamcp.toolsets.consumers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
 * Gerçek (ZooKeeper) Admin'i saran ve eski ZooKeeper broker'larının (Kafka 2.x) yanıtlarını taklit eden test
 * proxy'si:
 * <ul>
 * <li>listGroups: state ve type alanları yok (ListGroups v4 öncesi, Kafka &lt; 2.6; type ise v5 öncesi,
 * Kafka &lt; 3.8).</li>
 * <li>deleteConsumerGroupOffsets: OffsetDelete API'si yok (Kafka &lt; 2.4), AdminClient'ın verdiği gibi
 * UnsupportedVersionException.</li>
 * </ul>
 */
final class OldBrokerAdmin implements InvocationHandler {

	private final Admin real;

	private OldBrokerAdmin(Admin real) {
		this.real = real;
	}

	static Admin wrap(Admin real) {
		return (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] { Admin.class },
				new OldBrokerAdmin(real));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
		String name = method.getName();
		if (name.equals("listGroups")) {
			return listGroups(args == null || args.length == 0 ? new ListGroupsOptions() : (ListGroupsOptions) args[0]);
		}
		if (name.equals("deleteConsumerGroupOffsets")) {
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
				.map(g -> (Object) new GroupListing(g.groupId(), Optional.empty(), g.protocol(), Optional.empty()))
				.toList());
		});
		Constructor<ListGroupsResult> c = ListGroupsResult.class.getDeclaredConstructor(KafkaFuture.class);
		c.setAccessible(true);
		return c.newInstance(stripped);
	}

}

package io.github.srdrcn.kafkamcp.toolsets.diagnose;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.common.GroupType;

import io.github.srdrcn.kafkamcp.kafka.Futures;
import io.github.srdrcn.kafkamcp.kafka.KafkaErrors;
import io.github.srdrcn.kafkamcp.mcp.ToolCall;
import io.github.srdrcn.kafkamcp.mcp.ToolException;

/**
 * Time-bounded consumer group listing (a single listGroups with a request timeoutMs): classic + consumer protocol
 * (excluding share/streams and other groups with a protocol such as connect). Brokers that do not respond end up
 * in errors.
 */
public final class Groups {

	private Groups() {
	}

	/** groupId → listing (sorted) and broker errors. */
	public record Listing(Map<String, GroupListing> groups, List<String> errors) {

		public List<String> ids() {
			return new ArrayList<>(groups.keySet());
		}

	}

	/** Group listing within at most maxMillis; ToolException if there is no response at all. */
	public static Listing list(ToolCall call, long maxMillis) {
		long max = Math.max(500, Math.min(maxMillis, call.waitMillis()));
		ListGroupsResult r = call.admin().listGroups(new ListGroupsOptions().timeoutMs((int) max));
		Collection<GroupListing> valid = Futures.awaitAtMost(call, r.valid(), max + 500);
		List<String> errors = new ArrayList<>();
		try {
			Futures.awaitAtMost(call, r.errors(), 500).forEach(e -> errors.add(KafkaErrors.explain(e)));
		}
		catch (ToolException e) {
			errors.add(e.getMessage());
		}
		Map<String, GroupListing> out = new TreeMap<>();
		for (GroupListing g : valid) {
			GroupType type = g.type().orElse(GroupType.UNKNOWN);
			if (type == GroupType.SHARE || type == GroupType.STREAMS) {
				continue;
			}
			String protocol = g.protocol() == null ? "" : g.protocol();
			if (protocol.isEmpty() || "consumer".equals(protocol)) {
				out.put(g.groupId(), g);
			}
		}
		return new Listing(out, errors);
	}

}

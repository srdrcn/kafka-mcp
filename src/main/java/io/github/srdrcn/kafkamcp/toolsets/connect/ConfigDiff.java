package io.github.srdrcn.kafkamcp.toolsets.connect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import io.github.srdrcn.kafkamcp.mcp.Results;

/**
 * İki connector config'inin farkı (maskeli değerlerle): hedefte olup mevcutta olmayan (added), mevcutta
 * olup hedefte olmayan (removed) ve değeri farklı (changed) anahtarlar; boş gruplar çıktıya yazılmaz.
 * Verilen anahtarlar (ör. "name") yok sayılır.
 */
public final class ConfigDiff {

	private ConfigDiff() {
	}

	public static Map<String, Object> diff(Map<String, String> current, Map<String, String> target,
			Set<String> ignoredKeys) {
		Map<String, Object> added = new TreeMap<>();
		Map<String, Object> removed = new TreeMap<>();
		List<Map<String, Object>> changed = new ArrayList<>();
		Set<String> keys = new TreeSet<>(current.keySet());
		keys.addAll(target.keySet());
		for (String k : keys) {
			if (ignoredKeys.contains(k)) {
				continue;
			}
			boolean inCurrent = current.containsKey(k);
			boolean inTarget = target.containsKey(k);
			if (inCurrent && !inTarget) {
				removed.put(k, ConnectSecrets.mask(k, current.get(k)));
			}
			else if (!inCurrent) {
				added.put(k, ConnectSecrets.mask(k, target.get(k)));
			}
			else if (!Objects.equals(current.get(k), target.get(k))) {
				changed.add(Results.map("key", k, "current", ConnectSecrets.mask(k, current.get(k)), "target",
						ConnectSecrets.mask(k, target.get(k))));
			}
		}
		return Results.map("identical", added.isEmpty() && removed.isEmpty() && changed.isEmpty(), "addedCount",
				added.size(), "removedCount", removed.size(), "changedCount", changed.size(), "added",
				added.isEmpty() ? null : added, "removed", removed.isEmpty() ? null : removed, "changed",
				changed.isEmpty() ? null : changed);
	}

}

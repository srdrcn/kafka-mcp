package io.github.srdrcn.kafkamcp.support;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import io.github.srdrcn.kafkamcp.mcp.Toolset;

/**
 * Classpath'teki toolset adları ({@code toolsets/<ad>} paket adından). Testlerde "tüm toolset'ler açık"
 * config'i üretmek için kullanılır; toolset'in {@code name()} değerinin paket adıyla aynı olduğu
 * {@code ToolSchemaSnapshotTest}'te doğrulanır.
 */
public final class TestToolsets {

	/** Listeleme sırası (README'deki sıra). Burada olmayanlar sona alfabetik eklenir. */
	public static final List<String> ORDER = List.of("core", "messages", "consumers", "acl", "connect",
			"schemaregistry", "report", "diagnose");

	private TestToolsets() {
	}

	public static List<String> all() {
		ClassPathScanningCandidateComponentProvider scanner = new ClassPathScanningCandidateComponentProvider(false);
		scanner.addIncludeFilter(new AssignableTypeFilter(Toolset.class));
		List<String> names = new ArrayList<>();
		for (BeanDefinition bd : scanner.findCandidateComponents("io.github.srdrcn.kafkamcp.toolsets")) {
			String cls = bd.getBeanClassName();
			String pkg = cls.substring(0, cls.lastIndexOf('.'));
			String name = pkg.substring(pkg.lastIndexOf('.') + 1);
			if (!names.contains(name)) {
				names.add(name);
			}
		}
		names.sort(Comparator.comparingInt((String n) -> ORDER.contains(n) ? ORDER.indexOf(n) : ORDER.size())
			.thenComparing(Comparator.naturalOrder()));
		return names;
	}

}

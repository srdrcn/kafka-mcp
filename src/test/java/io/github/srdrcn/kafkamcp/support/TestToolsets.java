package io.github.srdrcn.kafkamcp.support;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AssignableTypeFilter;

import io.github.srdrcn.kafkamcp.mcp.Toolset;

/**
 * Toolset names on the classpath (from the {@code toolsets/<name>} package name). Used by tests to build an
 * "all toolsets enabled" config; {@code ToolSchemaSnapshotTest} checks that a toolset's {@code name()} matches
 * its package name.
 */
public final class TestToolsets {

	/** Listing order (the order in the README). Toolsets not listed here are appended alphabetically. */
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

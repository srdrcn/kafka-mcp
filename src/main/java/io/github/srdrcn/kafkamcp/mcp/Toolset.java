package io.github.srdrcn.kafkamcp.mcp;

import java.util.List;

/**
 * Bir toolset: config'teki {@code toolsets} listesiyle açılıp kapanan araç grubu. Her toolset
 * {@code toolsets/<ad>} paketinde bir Spring {@code @Component} olarak tanımlanır.
 */
public interface Toolset {

	/** Config'te kullanılan ad (ör. core, messages). */
	String name();

	/** Kısa Türkçe açıklama. */
	String description();

	/** Toolset'in tüm araçları (filtrelenmemiş). */
	List<ToolSpec> tools();

}

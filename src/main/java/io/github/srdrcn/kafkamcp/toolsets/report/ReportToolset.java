package io.github.srdrcn.kafkamcp.toolsets.report;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** report: cluster geneli (filo) raporları; tümü salt okunur, toplu admin istekleriyle. */
@Component
public class ReportToolset implements Toolset {

	private final ClusterReportTools clusterReportTools;

	private final TopicUsageTools topicUsageTools;

	private final AuditTools auditTools;

	private final NamingTools namingTools;

	private final InventoryTools inventoryTools;

	public ReportToolset(ClusterReportTools clusterReportTools, TopicUsageTools topicUsageTools, AuditTools auditTools,
			NamingTools namingTools, InventoryTools inventoryTools) {
		this.clusterReportTools = clusterReportTools;
		this.topicUsageTools = topicUsageTools;
		this.auditTools = auditTools;
		this.namingTools = namingTools;
		this.inventoryTools = inventoryTools;
	}

	@Override
	public String name() {
		return "report";
	}

	@Override
	public String description() {
		return "Cluster geneli raporlar: özet, en büyük/aktif topic'ler, boş/boşta topic'ler, denetimler, envanter";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(clusterReportTools.tools());
		tools.addAll(topicUsageTools.tools());
		tools.addAll(auditTools.tools());
		tools.addAll(namingTools.tools());
		tools.addAll(inventoryTools.tools());
		return tools;
	}

}

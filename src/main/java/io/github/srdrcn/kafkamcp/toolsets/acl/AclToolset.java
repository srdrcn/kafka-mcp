package io.github.srdrcn.kafkamcp.toolsets.acl;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.springframework.stereotype.Component;

import io.github.srdrcn.kafkamcp.mcp.ToolSpec;
import io.github.srdrcn.kafkamcp.mcp.Toolset;

/** acl: ACL, quota and security tools. */
@Component
public class AclToolset implements Toolset {

	/** Listing order: read tools first, then write tools. */
	private static final List<String> ORDER = List.of("acls_list", "acls_explain", "acls_who_can_access_topic",
			"acls_principal_permissions", "acls_audit", "quotas_list", "scram_users_list", "delegation_tokens_list",
			"kerberos_self_check", "security_summary", "acls_create", "quotas_update", "acls_delete");

	private final AclListTools listTools;

	private final AclAccessTools accessTools;

	private final AclWriteTools writeTools;

	private final QuotaTools quotaTools;

	private final SecurityTools securityTools;

	public AclToolset(AclListTools listTools, AclAccessTools accessTools, AclWriteTools writeTools,
			QuotaTools quotaTools, SecurityTools securityTools) {
		this.listTools = listTools;
		this.accessTools = accessTools;
		this.writeTools = writeTools;
		this.quotaTools = quotaTools;
		this.securityTools = securityTools;
	}

	@Override
	public String name() {
		return "acl";
	}

	@Override
	public String description() {
		return "ACL, quota, SCRAM, delegation token and Kerberos/security tools";
	}

	@Override
	public List<ToolSpec> tools() {
		List<ToolSpec> tools = new ArrayList<>();
		tools.addAll(listTools.tools());
		tools.addAll(accessTools.tools());
		tools.addAll(quotaTools.tools());
		tools.addAll(securityTools.tools());
		tools.addAll(writeTools.tools());
		tools.sort(Comparator.comparingInt((ToolSpec t) -> ORDER.contains(t.name()) ? ORDER.indexOf(t.name())
				: ORDER.size()));
		return tools;
	}

}

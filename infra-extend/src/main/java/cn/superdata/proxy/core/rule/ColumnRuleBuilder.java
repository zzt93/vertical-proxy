package cn.superdata.proxy.core.rule;

import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRuleBuilder;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRulesBuilderMaterials;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ColumnRuleBuilder implements SchemaRuleBuilder<ColumnRuleConfiguration> {
	@Override
	public ColumnRule build(SchemaRulesBuilderMaterials materials, ColumnRuleConfiguration config, Collection<ShardingSphereRule> builtRules) {
		HashMap<String, String> pk = new HashMap<>();
		pk.put("data_flow", "id");
		pk.put("data_flow_job", "data_flow_id");
		HashMap<String, Map<String, String>> logic = new HashMap<>();
		logic.put("data_flow", Collections.singletonMap("name", "name"));
		logic.put("data_flow_job", Collections.singletonMap("type", "type"));
		return new ColumnRule("id", pk, logic);
	}

	@Override
	public int getOrder() {
		return 1;
	}

	@Override
	public Class<ColumnRuleConfiguration> getTypeClass() {
		return ColumnRuleConfiguration.class;
	}
}

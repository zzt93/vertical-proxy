package cn.superdata.proxy.core.rule;

import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRuleBuilder;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRulesBuilderMaterials;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ShardingExtraRuleBuilder implements SchemaRuleBuilder<ShardingExtraConfiguration> {
	@Override
	public ShardingExtraRule build(SchemaRulesBuilderMaterials materials, ShardingExtraConfiguration config, Collection<ShardingSphereRule> builtRules) {
		Map<String, ColumnRule> m = new HashMap<>();
		for (Map.Entry<String, ColumnRuleConfiguration> e : config.getTables().entrySet()) {
			ColumnRuleConfiguration v = e.getValue();
			m.put(e.getKey(), new ColumnRule(v.getPkName(), v.getDataNodesPkName(), v.getLogicToActual()));
		}
		return new ShardingExtraRule(m);
	}

	@Override
	public int getOrder() {
		return 1;
	}

	@Override
	public Class<ShardingExtraConfiguration> getTypeClass() {
		return ShardingExtraConfiguration.class;
	}
}

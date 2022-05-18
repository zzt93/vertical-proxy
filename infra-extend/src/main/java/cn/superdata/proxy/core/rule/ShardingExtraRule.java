package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.rule.identifier.scope.SchemaRule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @see org.apache.shardingsphere.sharding.rule.TableRule
 * @see org.apache.shardingsphere.sharding.rule.ShardingRule
 */
@RequiredArgsConstructor
@Getter
public class ShardingExtraRule implements SchemaRule {

	private final Map<String, ColumnRule> tables;

	@Override
	public String getType() {
		return ShardingExtraRule.class.getSimpleName();
	}

	public Map<String, String> getLogicToActual(String logicTable, String actualTable) {
		return tables.get(logicTable).getLogicToActual(actualTable);
	}

	public String getPrimaryKey(String logicTable, String actualTable) {
		return tables.get(logicTable).getPrimaryKey(actualTable);
	}

	public String getLogicPrimaryKey(String logicTable) {
		return tables.get(logicTable).getLogicPrimaryKey();
	}

}

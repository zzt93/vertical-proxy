package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.infra.rule.identifier.scope.SchemaRule;
import org.apache.shardingsphere.infra.rule.identifier.type.TableContainedRule;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/**
 * @see org.apache.shardingsphere.sharding.rule.TableRule
 * @see org.apache.shardingsphere.sharding.rule.ShardingRule
 */
@RequiredArgsConstructor
public class ShardingExtraRule implements SchemaRule, TableContainedRule {

	private final Map<String, ColumnRule> tables;

	@Override
	public String getType() {
		return ShardingExtraRule.class.getSimpleName();
	}

	public Map<String, String> getLogicToActual(String logicTable, String actualTable) {
		return tables.get(logicTable).getLogicToActual(actualTable);
	}

	public Map<String, String> getLogicToActual(String logicTable) {
		return tables.get(logicTable).getLogicToActual();
	}

	public Map<String, String> getActualToLogic(String logicTable) {
		return tables.get(logicTable).getActualToLogic();
	}

	public String getPrimaryKey(String logicTable, String actualTable) {
		return tables.get(logicTable).getPrimaryKey(actualTable);
	}

	public String getLogicPrimaryKey(String logicTable) {
		return tables.get(logicTable).getLogicPrimaryKey();
	}

	public Optional<String> findLogicTableByActualTable(String actualTable) {
		for (Map.Entry<String, ColumnRule> e : tables.entrySet()) {
			ColumnRule value = e.getValue();
			if (value.hasActualTables(actualTable)) {
				return Optional.ofNullable(e.getKey());
			}
		}
		return Optional.empty();
	}

	@Override
	public Collection<String> getTables() {
		return tables.keySet();
	}
}

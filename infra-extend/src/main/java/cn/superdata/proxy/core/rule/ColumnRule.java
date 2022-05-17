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
public class ColumnRule implements SchemaRule {

	private final String logicPrimaryKey;
	private final Map<String, String> actualPrimaryKey;
	private final Map<String, Map<String, String>> logicToActual;

	@Override
	public String getType() {
		return ColumnRule.class.getSimpleName();
	}

	public Map<String, String> getLogicToActual(String actualTable) {
		Map<String, String> res = new HashMap<>(logicToActual.getOrDefault(actualTable, Collections.emptyMap()));
		res.put(logicPrimaryKey, actualPrimaryKey.get(actualTable));
		return res;
	}
}

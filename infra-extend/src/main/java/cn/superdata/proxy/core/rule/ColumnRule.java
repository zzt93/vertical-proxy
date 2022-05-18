package cn.superdata.proxy.core.rule;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.shardingsphere.infra.rule.identifier.scope.SchemaRule;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@AllArgsConstructor
@Getter
public class ColumnRule implements SchemaRule {
	private final String logicPrimaryKey;
	private final Map<String, String> primaryKey;
	private final Map<String, Map<String, String>> logicToActual;

	@Override
	public String getType() {
		return ColumnRule.class.getSimpleName();
	}

	public Map<String, String> getLogicToActual(String actualTable) {
		Map<String, String> res = new HashMap<>(logicToActual.getOrDefault(actualTable, Collections.emptyMap()));
		res.put(logicPrimaryKey, primaryKey.get(actualTable));
		return res;
	}

	public String getPrimaryKey(String actualTable) {
		return primaryKey.get(actualTable);
	}
}

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

	public Map<String, String> getLogicToActual() {
		Map<String, String> res = new HashMap<>();
		res.put(logicPrimaryKey, primaryKey.entrySet().iterator().next().getValue());
		for (Map.Entry<String, Map<String, String>> m : logicToActual.entrySet()) {
			for (Map.Entry<String, String> e : m.getValue().entrySet()) {
				res.put(e.getKey(), e.getValue());
			}
		}
		return res;
	}

	public Map<String, String> getActualToLogic() {
		Map<String, String> res = new HashMap<>();
		for (Map.Entry<String, String> e : primaryKey.entrySet()) {
			res.put(e.getValue(), logicPrimaryKey);
		}
		for (Map.Entry<String, Map<String, String>> m : logicToActual.entrySet()) {
			for (Map.Entry<String, String> e : m.getValue().entrySet()) {
				res.put(e.getValue(), e.getKey());
			}
		}
		return res;
	}

	public String getPrimaryKey(String actualTable) {
		return primaryKey.get(actualTable);
	}

	public boolean hasActualTables(String actualTable) {
		return primaryKey.containsKey(actualTable) || logicToActual.values().stream().anyMatch(m -> m.containsKey(actualTable));
	}
}

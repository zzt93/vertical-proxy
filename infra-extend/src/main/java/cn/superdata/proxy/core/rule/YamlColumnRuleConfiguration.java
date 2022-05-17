package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.YamlRuleConfiguration;
import org.apache.shardingsphere.sharding.yaml.config.rule.YamlTableRuleConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Setter
public class YamlColumnRuleConfiguration implements YamlRuleConfiguration {
	private String pkName;
	private Map<String, String> dataNodesPkName;
	private Map<String, Map<String, String>> logicToActual;

	@Override
	public Class<? extends RuleConfiguration> getRuleConfigurationType() {
		return ColumnRuleConfiguration.class;
	}
}

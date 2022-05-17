package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.yaml.config.pojo.YamlRuleConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Setter
public class YamlShardingExtraConfiguration implements YamlRuleConfiguration {
	private Map<String, YamlColumnRuleConfiguration> tables = new LinkedHashMap<>();

	@Override
	public Class<? extends RuleConfiguration> getRuleConfigurationType() {
		return ShardingExtraConfiguration.class;
	}
}

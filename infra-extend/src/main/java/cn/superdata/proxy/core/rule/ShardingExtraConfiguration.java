package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.config.function.DistributedRuleConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

@Getter
@Setter
public class ShardingExtraConfiguration implements DistributedRuleConfiguration {

	private Map<String, ColumnRuleConfiguration> tables = new LinkedHashMap<>();

}

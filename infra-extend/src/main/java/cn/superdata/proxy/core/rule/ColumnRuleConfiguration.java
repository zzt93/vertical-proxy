package cn.superdata.proxy.core.rule;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.infra.config.function.DistributedRuleConfiguration;

import java.util.Map;

@Getter
@Setter
public class ColumnRuleConfiguration implements DistributedRuleConfiguration {

	private String pkName;
	private Map<String, String> dataNodesPkName;
	private Map<String, Map<String, String>> logicToActual;

}

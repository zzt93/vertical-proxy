package cn.superdata.proxy.infra.rewrite;

import org.apache.shardingsphere.infra.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;
import org.apache.shardingsphere.sharding.rule.ShardingRule;

import java.util.Collection;
import java.util.LinkedList;

public class OverrideDefaultShardingSQLRewriteContextDecorator implements SQLRewriteContextDecorator<ShardingRule> {
	@Override
	public void decorate(ShardingRule rule, ConfigurationProperties props, SQLRewriteContext sqlRewriteContext, RouteContext routeContext) {

	}

	@Override
	public int getOrder() {
		return ShardingOrder.ALGORITHM_PROVIDER_ORDER;
	}

	@Override
	public Class<ShardingRule> getTypeClass() {
		return ShardingRule.class;
	}
}

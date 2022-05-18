package cn.superdata.proxy.infra.rewrite;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import org.apache.shardingsphere.infra.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.infra.rewrite.context.SQLRewriteContextDecorator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.SQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;
import org.apache.shardingsphere.sharding.rewrite.token.generator.impl.OrderByTokenGenerator;

import java.util.Collection;
import java.util.LinkedList;

public class NewShardingSQLRewriteContextDecorator implements SQLRewriteContextDecorator<ShardingExtraRule> {
	@Override
	public void decorate(ShardingExtraRule rule, ConfigurationProperties props, SQLRewriteContext sqlRewriteContext, RouteContext routeContext) {
		Collection<SQLTokenGenerator> result = new LinkedList<>();
		result.add(new ProjectionTokenGenerator(rule));
		result.add(new FromTokenGenerator());
		result.add(new NewOrderByTokenGenerator(rule));
		result.add(new WhereTokenGenerator(rule));

		for (SQLTokenGenerator each : result) {
			if (each instanceof RouteContextAware) {
				((RouteContextAware) each).setRouteContext(routeContext);
			}
		}
		sqlRewriteContext.addSQLTokenGenerators(result);
	}

	@Override
	public int getOrder() {
		return ShardingOrder.ORDER-1;
	}

	@Override
	public Class<ShardingExtraRule> getTypeClass() {
		return ShardingExtraRule.class;
	}
}

package cn.superdata.proxy.infra.route;

import cn.superdata.proxy.infra.rewrite.ColumnSegments;
import org.apache.shardingsphere.infra.binder.LogicSQL;
import org.apache.shardingsphere.infra.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.route.SQLRouter;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;
import org.apache.shardingsphere.sharding.route.engine.ShardingSQLRouter;
import org.apache.shardingsphere.sharding.route.engine.condition.ShardingConditions;
import org.apache.shardingsphere.sharding.route.engine.type.broadcast.ShardingDataSourceGroupBroadcastRoutingEngine;
import org.apache.shardingsphere.sharding.route.engine.type.standard.ShardingStandardRoutingEngine;
import org.apache.shardingsphere.sharding.rule.ShardingRule;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dal.MySQLExplainStatement;

import java.util.Collections;

public class ShardingExtraRouter implements SQLRouter<ShardingRule> {

	private final ShardingSQLRouter shardingSQLRouter;

	public ShardingExtraRouter() {
		shardingSQLRouter = new ShardingSQLRouter();
	}

	@Override
	public RouteContext createRouteContext(LogicSQL logicSQL, ShardingSphereMetaData metaData, ShardingRule rule, ConfigurationProperties props) {
		if (logicSQL.getSqlStatementContext().getSqlStatement() instanceof MySQLExplainStatement) {
			String singleLogicTable = ColumnSegments.getSingleLogicTable(logicSQL.getSqlStatementContext());
			// empty sharding condition => route to all unit
			ShardingConditions shardingConditions = new ShardingConditions(Collections.emptyList(), logicSQL.getSqlStatementContext(), rule);
			return new ShardingStandardRoutingEngine(singleLogicTable, shardingConditions, props).route(rule);
		}
		return shardingSQLRouter.createRouteContext(logicSQL, metaData, rule, props);
	}

	@Override
	public void decorateRouteContext(RouteContext routeContext, LogicSQL logicSQL, ShardingSphereMetaData metaData, ShardingRule rule, ConfigurationProperties props) {

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

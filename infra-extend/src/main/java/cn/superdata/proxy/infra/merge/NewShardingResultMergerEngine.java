package cn.superdata.proxy.infra.merge;

import cn.superdata.proxy.core.rule.ColumnRule;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.config.properties.ConfigurationProperties;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.merge.engine.merger.ResultMerger;
import org.apache.shardingsphere.infra.merge.engine.merger.ResultMergerEngine;
import org.apache.shardingsphere.infra.merge.engine.merger.impl.TransparentResultMerger;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dal.DALStatement;

public class NewShardingResultMergerEngine implements ResultMergerEngine<ColumnRule> {
	@Override
	public ResultMerger newInstance(String schemaName, DatabaseType databaseType, ColumnRule rule, ConfigurationProperties props, SQLStatementContext<?> sqlStatementContext) {
		if (sqlStatementContext instanceof SelectStatementContext) {
			return new NewShardingDQLResultMerger(databaseType, rule);
		}
		if (sqlStatementContext.getSqlStatement() instanceof DALStatement) {
//			return new ShardingDALResultMerger(schemaName, rule);
		}
		return new TransparentResultMerger();
	}

	@Override
	public int getOrder() {
		return ShardingOrder.ORDER-1;
	}

	@Override
	public Class<ColumnRule> getTypeClass() {
		return ColumnRule.class;
	}
}

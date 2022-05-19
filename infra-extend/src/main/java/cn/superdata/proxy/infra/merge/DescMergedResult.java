package cn.superdata.proxy.infra.merge;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import cn.superdata.proxy.infra.rewrite.ColumnSegments;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.impl.memory.MemoryMergedResult;
import org.apache.shardingsphere.infra.merge.result.impl.memory.MemoryQueryResultRow;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.sharding.rule.TableRule;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class DescMergedResult extends MemoryMergedResult<ShardingExtraRule> {

	public static final int FIELD_NAME = 1;

	protected DescMergedResult(ShardingExtraRule rule, ShardingSphereSchema schema, SQLStatementContext sqlStatementContext, List<QueryResult> list) throws SQLException {
		super(rule, schema, sqlStatementContext, list);
	}

	@Override
	protected List<MemoryQueryResultRow> init(ShardingExtraRule rule, ShardingSphereSchema schema, SQLStatementContext sqlStatementContext, List<QueryResult> list) throws SQLException {
		List<MemoryQueryResultRow> res = new ArrayList<>();
		String singleLogicTable = ColumnSegments.getSingleLogicTable(sqlStatementContext);
		TableMetaData tableMetaData = schema.get(singleLogicTable);
		HashMap<String, MemoryQueryResultRow> m = new HashMap<>();
		for (QueryResult queryResult : list) {
			while (queryResult.next()) {
				MemoryQueryResultRow memoryResultSetRow = new MemoryQueryResultRow(queryResult);
				m.put(memoryResultSetRow.getCell(FIELD_NAME).toString(), memoryResultSetRow);
			}
		}
		Map<String, String> logicToActual = rule.getLogicToActual(singleLogicTable);
		for (Map.Entry<String, ColumnMetaData> e : tableMetaData.getColumns().entrySet()) {
			ColumnMetaData value = e.getValue();
			String logicName = value.getName();
			String actual = logicToActual.get(logicName);
			MemoryQueryResultRow row = m.get(actual);
			if (row != null) {
				row.setCell(FIELD_NAME, logicName);
				res.add(row);
			}
		}
		return res;
	}
}

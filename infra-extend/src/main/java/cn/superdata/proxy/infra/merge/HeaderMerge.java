package cn.superdata.proxy.infra.merge;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import cn.superdata.proxy.infra.rewrite.ColumnSegments;
import lombok.Data;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeaderMerge {

	// columnIndex start from 1
	public static final int FIRST_INDEX = 1;

	public static MappedQueryResults projectionMapping(SQLStatementContext<?> sql, List<QueryResult> queryResults, ShardingExtraRule shardingExtraRule) throws SQLException {
		if (shardingExtraRule != null && hasSelectExpandProjections(sql)) {
			SelectStatementContext sqlStatementContext = (SelectStatementContext) sql;
			String singleLogicTable = ColumnSegments.getSingleLogicTable(sqlStatementContext);
			ProjectionsSegment projections = sqlStatementContext.getSqlStatement().getProjections();
			Map<Integer, RouteUnitIndex> l = new HashMap<>();
			int routeUnitCount = 0;
			for (QueryResult queryResult : queryResults) {
				int colIndex = 1;
				String singleActualTable = queryResult.getMetaData().getTableName(FIRST_INDEX);
				Map<String, String> logicToActual = shardingExtraRule.getLogicToActual(singleLogicTable, singleActualTable);
				ArrayList<String> projectionStr = ColumnSegments.projectionWithNull(projections, logicToActual, shardingExtraRule.getLogicPrimaryKey(singleLogicTable));
				for (int i = 0; i < projectionStr.size(); i++) {
					if (projectionStr.get(i) != null) {
						l.put(i+1, new RouteUnitIndex(colIndex++, routeUnitCount));
					}
				}
				routeUnitCount++;
			}
			return new MappedQueryResults(l);
		} else {
			return new MappedQueryResults(0, getColumnCount(queryResults.get(0), sql));
		}
	}

	@Data
	public static class MappedQueryResults {
		Map<Integer, RouteUnitIndex> routeUnitIndices;
		private int mappedResultIndex;
		private Integer columnCount;

		public MappedQueryResults(Map<Integer, RouteUnitIndex> routeUnitIndices) {
			this.routeUnitIndices = routeUnitIndices;
		}

		public MappedQueryResults(int mappedResultIndex, int columnCount) {
			this.mappedResultIndex = mappedResultIndex;
			this.columnCount = columnCount;
		}

		/**
		 * @see ColumnSegments#projectionWithNull(ProjectionsSegment, Map, String)
		 */
		public int getColumnCount() {
			return columnCount == null ? routeUnitIndices.size() - 1 /* -1 for added primary key */ : columnCount;
		}

		/**
		 * @see ColumnSegments#projectionWithNull(ProjectionsSegment, Map, String)
		 */
		public RouteUnitIndex get(int columnIndex) {
			if (routeUnitIndices == null) {
				return new RouteUnitIndex(columnIndex, mappedResultIndex);
			} else {
				return routeUnitIndices.get(columnIndex + 1/* +1 for added primary key */);
			}
		}
	}

	private static int getColumnCount(final QueryResult queryResultSample, SQLStatementContext<?> sqlStatementContext) throws SQLException {
		return hasSelectExpandProjections(sqlStatementContext)
				? ((SelectStatementContext) sqlStatementContext).getProjectionsContext().getExpandProjections().size() : queryResultSample.getMetaData().getColumnCount();
	}

	private static boolean hasSelectExpandProjections(final SQLStatementContext<?> sqlStatementContext) {
		return sqlStatementContext instanceof SelectStatementContext && !((SelectStatementContext) sqlStatementContext).getProjectionsContext().getExpandProjections().isEmpty()
				&& !((SelectStatementContext) sqlStatementContext).isContainsSubquery();
	}
}

package cn.superdata.proxy.infra.merge;

import cn.superdata.proxy.core.rule.ColumnRule;
import lombok.Data;
import org.apache.shardingsphere.infra.binder.segment.select.projection.Projection;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResultMetaData;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HeaderMerge {

	// columnIndex start from 1
	public static final int FIRST_INDEX = 1;

	@Data
	public static class MappedQueryResults {
		List<MappedQueryResult> mappedQueryResults;
		private int mappedResultIndex;
		private int columnCount;
		private Map<Integer, Integer> pkIndex;

		public MappedQueryResults(List<MappedQueryResult> mappedQueryResults, int columnCount, Map<Integer, Integer> pkIndex) {
			this.mappedQueryResults = mappedQueryResults;
			this.columnCount = columnCount;
			this.pkIndex = pkIndex;
		}

		public MappedQueryResults(int mappedResultIndex, int columnCount) {
			this.mappedResultIndex = mappedResultIndex;
			this.columnCount = columnCount;
		}

		public MappedQueryResult get(int columnIndex) {
			if (mappedQueryResults == null) {
				return new MappedQueryResult(columnIndex, mappedResultIndex);
			} else {
				return mappedQueryResults.get(columnIndex - 1);
			}
		}
	}

	@Data
	public static class MappedQueryResult {
		private final int mappedColIndex;
		private final int mappedResultIndex;

		public MappedQueryResult(int mappedColIndex, int mappedResultIndex) {
			this.mappedColIndex = mappedColIndex;
			this.mappedResultIndex = mappedResultIndex;
		}

	}

	public static MappedQueryResults mappedQueryResults(List<QueryResult> queryResults, SQLStatementContext<?> sqlStatementContext1, ColumnRule columnRule) throws SQLException {
		if (columnRule != null) {
			if (hasSelectExpandProjections(sqlStatementContext1)) {
				SelectStatementContext sqlStatementContext = (SelectStatementContext) sqlStatementContext1;
				List<Projection> expandProjections = sqlStatementContext.getProjectionsContext().getExpandProjections();
				List<MappedQueryResult> l = new ArrayList<>(expandProjections.size());
				Map<Integer, Integer> pkIndex = new HashMap<>();
				for (Projection projection : expandProjections) {
					String columnLabel = projection.getColumnLabel();
					MappedQueryResult moreOne = null;
					Map<Integer, Integer> tmpM = new HashMap<>();
					for (int x = 0; x < queryResults.size(); x++) {
						QueryResult queryResult = queryResults.get(x);
						QueryResultMetaData resultMetaData = queryResult.getMetaData();
						String tableName = resultMetaData.getTableName(FIRST_INDEX);
						Map<String, String> logicToActual = columnRule.getLogicToActual(tableName);
						String actual = logicToActual.get(columnLabel);
						if (actual != null) {
							for (int i = FIRST_INDEX; i < FIRST_INDEX + resultMetaData.getColumnCount(); i++) {
								if (resultMetaData.getColumnName(i).equals(actual)) {
									moreOne = new MappedQueryResult(i, x);
									tmpM.put(x, i);
									break;
								}
							}
						}
					}
					if (tmpM.size() > 1) {
						pkIndex.putAll(tmpM);
					}
					l.add(moreOne);
				}
				return new MappedQueryResults(l, getColumnCount(queryResults.get(0), sqlStatementContext), pkIndex);
			}
		}
		return new MappedQueryResults(0, getColumnCount(queryResults.get(0), sqlStatementContext1));
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

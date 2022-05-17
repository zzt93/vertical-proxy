package cn.superdata.proxy.infra.merge;

import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NewShardingMergedResult implements MergedResult {
	private final HeaderMerge.MappedQueryResults mappedQueryResults;
	private final List<QueryResult> queryResults;
	private final boolean[] used;
	private Map<Integer, Integer> pkIndex;
	private Class<?> pkType;
	private Comparator pkComparator;
	private boolean wasNull;

	public NewShardingMergedResult(List<QueryResult> queryResults, HeaderMerge.MappedQueryResults mappedQueryResults) {
		this.mappedQueryResults = mappedQueryResults;
		this.queryResults = queryResults;
		used = new boolean[queryResults.size()];
		Arrays.fill(used, true);
		pkType = Object.class;
		pkComparator = Comparator.naturalOrder();
		pkIndex = mappedQueryResults.getPkIndex();
	}

	@Override
	public boolean next() throws SQLException {
		boolean next = false;
		for (int i = 0; i < queryResults.size(); i++) {
			if (used[i]) {
				next = queryResults.get(i).next() || next;
				used[i] = false;
			}
		}
		if (!next) return false;

		Object pk = null;
		for (int i = 0; i < queryResults.size(); i++) {
			Object value = queryResults.get(i).getValue(pkIndex.get(i), pkType);
			if (i == 0) {
				pk = value;
			} else {
				pk = pkComparator.compare(pk, value) < 0 ? pk : value;
			}
		}
		for (int i = 0; i < queryResults.size(); i++) {
			Object value = queryResults.get(i).getValue(pkIndex.get(i), pkType);
			if (pkComparator.compare(value, pk) == 0) {
				used[i] = true;
			}
		}
		return true;
	}

	@Override
	public final Object getValue(final int columnIndex, final Class<?> type) throws SQLException {
		Optional<QueryResult> result = getCurrentQueryResult(columnIndex);
		HeaderMerge.MappedQueryResult mappedQueryResult = mappedQueryResults.get(columnIndex);
		return result.isPresent() ? result.get().getValue(mappedQueryResult.getMappedColIndex(), type) : null;
	}

	@Override
	public final Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) throws SQLException {
		Optional<QueryResult> result = getCurrentQueryResult(columnIndex);
		HeaderMerge.MappedQueryResult mappedQueryResult = mappedQueryResults.get(columnIndex);
		return result.isPresent() ? result.get().getCalendarValue(mappedQueryResult.getMappedColIndex(), type, calendar) : null;
	}

	@Override
	public final InputStream getInputStream(final int columnIndex, final String type) throws SQLException {
		Optional<QueryResult> result = getCurrentQueryResult(columnIndex);
		HeaderMerge.MappedQueryResult mappedQueryResult = mappedQueryResults.get(columnIndex);
		return result.isPresent() ? result.get().getInputStream(mappedQueryResult.getMappedColIndex(), type) : null;
	}

	@Override
	public final boolean wasNull() throws SQLException {
		return wasNull;
	}

	private Optional<QueryResult> getCurrentQueryResult(int columnIndex) throws SQLException {
		HeaderMerge.MappedQueryResult mappedQueryResult = mappedQueryResults.get(columnIndex);
		int mappedResultIndex = mappedQueryResult.getMappedResultIndex();
		if (used[mappedResultIndex]) {
			QueryResult value = queryResults.get(mappedResultIndex);
			wasNull = value.wasNull();
			return Optional.of(value);
		}
		return Optional.empty();
	}

}

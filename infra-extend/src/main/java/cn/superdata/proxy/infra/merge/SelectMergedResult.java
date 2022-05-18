package cn.superdata.proxy.infra.merge;

import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.sql.parser.sql.common.constant.OrderDirection;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class SelectMergedResult implements MergedResult {
	public static final int PK_COLUMN_INDEX = 1;
	private final HeaderMerge.MappedQueryResults mappedQueryResults;
	private final List<QueryResult> queryResults;
	private final boolean[] resultInUse;
	private final Class<?> pkType;
	private final Comparator pkComparator;
	private boolean wasNull;

	public SelectMergedResult(List<QueryResult> queryResults, HeaderMerge.MappedQueryResults mappedQueryResults, OrderDirection orderDirection) {
		this.mappedQueryResults = mappedQueryResults;
		this.queryResults = queryResults;
		resultInUse = new boolean[queryResults.size()];
		Arrays.fill(resultInUse, true);
		pkType = Object.class;
		pkComparator = Comparator.nullsLast(orderDirection == OrderDirection.ASC ? Comparator.naturalOrder() : Comparator.naturalOrder().reversed());
	}

	@Override
	public boolean next() throws SQLException {
		boolean next = false;
		boolean[] resultValid = new boolean[queryResults.size()];
		for (int i = 0; i < queryResults.size(); i++) {
			if (resultInUse[i]) { // result just used before next()
				resultValid[i] = queryResults.get(i).next();
				next = resultValid[i] || next;
				resultInUse[i] = false;
			}
		}
		if (!next) return false;

		Object pk = null;
		for (int i = 0; i < queryResults.size(); i++) {
			if (!resultValid[i]) continue;
			Object value = queryResults.get(i).getValue(PK_COLUMN_INDEX, pkType);
			if (i == 0) {
				pk = value;
			} else {
				pk = pkComparator.compare(pk, value) < 0 ? pk : value;
			}
		}
		for (int i = 0; i < queryResults.size(); i++) {
			if (!resultValid[i]) continue;
			Object value = queryResults.get(i).getValue(PK_COLUMN_INDEX, pkType);
			if (pkComparator.compare(value, pk) == 0) { // resultValid && key match
				resultInUse[i] = true;
			}
		}
		return true;
	}

	@Override
	public final Object getValue(final int columnIndex, final Class<?> type) throws SQLException {
		RouteUnitIndex routeUnitIndex = mappedQueryResults.get(columnIndex);
		Optional<QueryResult> result = getCurrentQueryResult(mappedQueryResults.get(columnIndex));
		if (routeUnitIndex.getMore() == null) {
			return result.isPresent() ? result.get().getValue(routeUnitIndex.getColIndexInRouteUnit(), type) : null;
		} else {
			Object value = null;
			do {
				result = getCurrentQueryResult(routeUnitIndex);
				if (result.isPresent()) {
					value = result.get().getValue(routeUnitIndex.getColIndexInRouteUnit(), type);
				}
				routeUnitIndex = routeUnitIndex.getMore();
			} while (value == null && routeUnitIndex != null);
			return value;
		}
	}

	@Override
	public final Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) throws SQLException {
		RouteUnitIndex routeUnitIndex = mappedQueryResults.get(columnIndex);
		Optional<QueryResult> result = getCurrentQueryResult(routeUnitIndex);
		return result.isPresent() ? result.get().getCalendarValue(routeUnitIndex.getColIndexInRouteUnit(), type, calendar) : null;
	}

	@Override
	public final InputStream getInputStream(final int columnIndex, final String type) throws SQLException {
		RouteUnitIndex routeUnitIndex = mappedQueryResults.get(columnIndex);
		Optional<QueryResult> result = getCurrentQueryResult(routeUnitIndex);
		return result.isPresent() ? result.get().getInputStream(routeUnitIndex.getColIndexInRouteUnit(), type) : null;
	}

	@Override
	public final boolean wasNull() throws SQLException {
		return wasNull;
	}

	private Optional<QueryResult> getCurrentQueryResult(RouteUnitIndex mappedQueryResult) throws SQLException {
		int routeUnitIndex = mappedQueryResult.getRouteUnitIndex();
		if (resultInUse[routeUnitIndex]) {
			QueryResult value = queryResults.get(routeUnitIndex);
			wasNull = value.wasNull();
			return Optional.of(value);
		}
		return Optional.empty();
	}

}

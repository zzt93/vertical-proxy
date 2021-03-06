package cn.superdata.proxy.infra.rewrite;

import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.column.ColumnSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.BetweenExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.BinaryOperationExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ExistsSubqueryExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.FunctionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.InExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ListExpression;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.complex.CommonExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.simple.LiteralExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.subquery.SubqueryExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ColumnProjectionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ExpressionProjectionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ShorthandProjectionSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ColumnSegments {
	public static List<String> projection(ProjectionsSegment projections, Map<String, String> logicToActual, String logicPrimaryKey) {
		return projectionWithNull(projections, logicToActual, logicPrimaryKey).stream().filter(Objects::nonNull).collect(Collectors.toList());
	}

	public static ArrayList<String> projectionWithNull(ProjectionsSegment projections, Map<String, String> logicToActual, String logicPrimaryKey) {
		ArrayList<String> res = new ArrayList<>();
		// always add primary key for result merge
		res.add(logicToActual.get(logicPrimaryKey));
		for (ProjectionSegment segment : projections.getProjections()) {
			if (segment instanceof ExpressionSegment) {
				res.add(extract((ExpressionSegment) segment, logicToActual));
			} else if (segment instanceof ColumnProjectionSegment) {
				String col = ((ColumnProjectionSegment) segment).getColumn().getIdentifier().getValue();
				res.add(logicToActual.get(col));
			} else if (segment instanceof ShorthandProjectionSegment) {
				// TODO: 2022/5/13 ShorthandProjection.getActual
				res.add("*");
			} else {
				throw new UnsupportedOperationException("unsupported TableSegment type: " + segment.getClass());
			}
		}
		return res;
	}

	public static String extract(ExpressionSegment segment, Map<String, String> colMap) {
		LinkedList<String> q = new LinkedList<>();
		build(segment, colMap, q);
		return q.poll();
	}

	private static void build(ExpressionSegment segment, Map<String, String> colMap, LinkedList<String> q) {
		if (null == segment) {
			return;
		}
		if (segment instanceof LiteralExpressionSegment) {
			Object literals = ((LiteralExpressionSegment) segment).getLiterals();
			if (literals instanceof String) {
				q.add("'" + literals + "'");
			} else {
				q.add(literals.toString());
			}
		} else if (segment instanceof ColumnSegment) {
			q.add(colMap.get(((ColumnSegment) segment).getIdentifier().getValue()));
		} else if (segment instanceof CommonExpressionSegment) {
		} else if (segment instanceof ListExpression) {
		} else if (segment instanceof BinaryOperationExpression) {
			build(((BinaryOperationExpression) segment).getRight(), colMap, q);
			build(((BinaryOperationExpression) segment).getLeft(), colMap, q);
			String left = q.pollLast();
			String right = q.pollLast();
			String operator = ((BinaryOperationExpression) segment).getOperator();
			if (left == null || right == null) {
				if (operator.equals("and") || operator.equals("or")) {
					q.add(left == null ? right : left);
				} else {
					q.add(null);
				}
			} else {
				q.add(left + " " + operator + " " + right);
			}
		} else if (segment instanceof ExistsSubqueryExpression) {
		} else if (segment instanceof SubqueryExpressionSegment) {
		} else if (segment instanceof InExpression) {
		} else if (segment instanceof BetweenExpression) {
		} else if (segment instanceof FunctionSegment) {
			Collection<ExpressionSegment> parameters = ((FunctionSegment) segment).getParameters();
			for (ExpressionSegment parameter : parameters) {
				build(parameter, colMap, q);
			}
			List<String> reversed = new ArrayList<>(parameters.size());
			for (ExpressionSegment ignored : parameters) {
				String e = q.pollLast();
				if (e == null) {
					q.add(null);
					return;
				}
				reversed.add(e);
			}
			StringJoiner sj = new StringJoiner(",", ((FunctionSegment) segment).getFunctionName() + "(", ")");
			for (int i = reversed.size() - 1; i >= 0; i--) {
				String s = reversed.get(i);
				sj.add(s);
			}
			q.add(sj.toString());
		} else if (segment instanceof ExpressionProjectionSegment) {
			build(((ExpressionProjectionSegment) segment).getExpr(), colMap, q);
		} else {
			throw new UnsupportedOperationException("unsupported TableSegment type: " + segment.getClass());
		}
	}

	public static String getSingleActualTable(SQLStatementContext sqlStatementContext, RouteUnit routeUnit) {
		Collection<String> tableNames = sqlStatementContext.getTablesContext().getTableNames();
		if (tableNames.size() != 1) {
			throw new UnsupportedOperationException();
		}
		Set<String> actualTableNames = routeUnit.getActualTableNames(tableNames.iterator().next());
		if (actualTableNames.size() != 1) {
			throw new UnsupportedOperationException();
		}
		return actualTableNames.iterator().next();
	}

	public static String getSingleLogicTable(SQLStatementContext sqlStatementContext) {
		Collection<String> tableNames = sqlStatementContext.getTablesContext().getTableNames();
		if (tableNames.size() != 1) {
			throw new UnsupportedOperationException();
		}
		return tableNames.iterator().next();
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.superdata.proxy.infra.rewrite;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.OptionalSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.constant.OrderDirection;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.order.OrderBySegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.order.item.OrderByItemSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.SimpleTableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.handler.dml.SelectStatementHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Projections token generator.
 */
@Setter
@Slf4j
public final class NewOrderByTokenGenerator implements OptionalSQLTokenGenerator<SelectStatementContext>, RouteContextAware {

	private final ShardingExtraRule rule;
	private RouteContext routeContext;

	public NewOrderByTokenGenerator(ShardingExtraRule rule) {
		this.rule = rule;
	}

	@Override
	public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
		return sqlStatementContext instanceof SelectStatementContext;
	}

	@Override
	public OrderByToken generateSQLToken(SelectStatementContext selectStatementContext) {
		Optional<OrderBySegment> orderBySegment = selectStatementContext.getSqlStatement().getOrderBy();
		OrderDirection dir = getOrderDirection(selectStatementContext);
		Map<RouteUnit, String> m = new HashMap<>();
		for (RouteUnit routeUnit : routeContext.getRouteUnits()) {
			String singleActualTable = ColumnSegments.getSingleActualTable(selectStatementContext, routeUnit);
			String singleLogicTable = ColumnSegments.getSingleLogicTable(selectStatementContext);
			String pk = rule.getPrimaryKey(singleLogicTable, singleActualTable);
			m.put(routeUnit, pk);
		}
		int startIndex = getGenerateOrderByStartIndex(selectStatementContext);
		return new OrderByToken(startIndex, orderBySegment.map(OrderBySegment::getStopIndex).orElse(startIndex), m, dir);
	}

	public OrderDirection getOrderDirection(SelectStatementContext selectStatementContext) {
		Optional<OrderBySegment> orderBySegment = selectStatementContext.getSqlStatement().getOrderBy();
		Collection<OrderByItemSegment> items = orderBySegment.map(OrderBySegment::getOrderByItems).orElse(Collections.emptyList());
		OrderDirection dir = OrderDirection.ASC;
		if (items.size() != 1) {
			if (items.size() > 1) log.warn("Ignore multiple order by clause: {}", selectStatementContext.getSqlStatement());
		} else {
			dir = items.iterator().next().getOrderDirection();
		}
		return dir;
	}

	private int getGenerateOrderByStartIndex(final SelectStatementContext selectStatementContext) {
		SelectStatement sqlStatement = selectStatementContext.getSqlStatement();
		int stopIndex;
		if (SelectStatementHandler.getWindowSegment(sqlStatement).isPresent()) {
			stopIndex = SelectStatementHandler.getWindowSegment(sqlStatement).get().getStopIndex();
		} else if (sqlStatement.getHaving().isPresent()) {
			stopIndex = sqlStatement.getHaving().get().getStopIndex();
		} else if (sqlStatement.getGroupBy().isPresent()) {
			stopIndex = sqlStatement.getGroupBy().get().getStopIndex();
		} else if (sqlStatement.getWhere().isPresent()) {
			stopIndex = sqlStatement.getWhere().get().getStopIndex();
		} else {
			stopIndex = selectStatementContext.getAllTables().stream().mapToInt(SimpleTableSegment::getStopIndex).max().orElse(0);
		}
		return stopIndex + 1;
	}

}

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
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.OptionalSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.SQLToken;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.expr.ExpressionSegment;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.predicate.WhereSegment;

import java.util.HashMap;
import java.util.Map;

/**
 * Projections token generator.
 */
@Setter
public final class WhereTokenGenerator implements OptionalSQLTokenGenerator<SelectStatementContext>, RouteContextAware {

    private final ShardingExtraRule rule;
    private RouteContext routeContext;

    public WhereTokenGenerator(ShardingExtraRule rule) {
        this.rule = rule;
    }

    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && ((SelectStatementContext)sqlStatementContext).getSqlStatement().getWhere().isPresent()
        && !getDerivedProjectionTexts((SelectStatementContext) sqlStatementContext).isEmpty();
    }

    private Map<RouteUnit, String> getDerivedProjectionTexts(final SelectStatementContext selectStatementContext) {
        WhereSegment whereSegment = selectStatementContext.getSqlStatement().getWhere().get();
        Map<RouteUnit, String> result = new HashMap<>();
        for (RouteUnit routeUnit : routeContext.getRouteUnits()) {
            Map<String, String> colMap = getLogicAndActualColumns(selectStatementContext, routeUnit);
            String condition = ColumnSegments.extract(whereSegment.getExpr(), colMap);
            if (condition != null) {
                result.put(routeUnit, condition);
            }
        }
        return result;
    }

    private Map<String, String> getLogicAndActualColumns(SelectStatementContext sqlStatementContext, final RouteUnit routeUnit) {
        String actualTable = ColumnSegments.getSingleActualTable(sqlStatementContext, routeUnit);
        String singleLogicTable = ColumnSegments.getSingleLogicTable(sqlStatementContext);
        return rule.getLogicToActual(singleLogicTable, actualTable);
    }

    @Override
    public SQLToken generateSQLToken(SelectStatementContext selectStatementContext) {
        Map<RouteUnit, String> derivedProjectionTexts = getDerivedProjectionTexts(selectStatementContext);
        ExpressionSegment whereSegment = selectStatementContext.getSqlStatement().getWhere().get().getExpr();
        return new WhereToken(whereSegment.getStartIndex(), whereSegment.getStopIndex(), derivedProjectionTexts);
    }
}

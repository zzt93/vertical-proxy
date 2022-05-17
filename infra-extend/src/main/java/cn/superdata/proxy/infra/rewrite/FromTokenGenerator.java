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

import lombok.Setter;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dal.ShowColumnsStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.OptionalSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.SQLToken;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.segment.generic.table.TableSegment;
import org.apache.shardingsphere.sql.parser.sql.common.statement.dml.SelectStatement;
import org.apache.shardingsphere.sql.parser.sql.dialect.statement.mysql.dal.MySQLShowColumnsStatement;

import java.util.HashMap;
import java.util.Map;

/**
 * Projections token generator.
 */
@Setter
public final class FromTokenGenerator implements OptionalSQLTokenGenerator<SQLStatementContext>, RouteContextAware {

    private RouteContext routeContext;

    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return !getDerivedProjectionTexts(sqlStatementContext).isEmpty();
    }

    private Map<RouteUnit, String> getDerivedProjectionTexts(final SQLStatementContext selectStatementContext) {
        Map<RouteUnit, String> result = new HashMap<>();
        for (RouteUnit routeUnit : routeContext.getRouteUnits()) {
            String singleActualTable = ColumnSegments.getSingleActualTable(selectStatementContext, routeUnit);
            result.put(routeUnit, singleActualTable);
        }
        return result;
    }

    @Override
    public FromToken generateSQLToken(SQLStatementContext sqlStatementContext) {
        if (sqlStatementContext instanceof SelectStatementContext) {
            TableSegment tableSegment = ((SelectStatement) sqlStatementContext.getSqlStatement()).getFrom();
            return new FromToken(tableSegment.getStartIndex(), tableSegment.getStopIndex(), getDerivedProjectionTexts(sqlStatementContext));
        } else if (sqlStatementContext instanceof ShowColumnsStatementContext) {
            TableSegment tableSegment = ((MySQLShowColumnsStatement) sqlStatementContext.getSqlStatement()).getTable();
            return new FromToken(tableSegment.getStartIndex(), tableSegment.getStopIndex(), getDerivedProjectionTexts(sqlStatementContext));
        } else {
            throw new UnsupportedOperationException();
        }
    }

}

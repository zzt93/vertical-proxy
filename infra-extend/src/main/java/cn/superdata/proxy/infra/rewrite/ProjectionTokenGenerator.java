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
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.CollectionSQLTokenGenerator;
import org.apache.shardingsphere.infra.rewrite.sql.token.generator.aware.RouteContextAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.SQLToken;
import org.apache.shardingsphere.infra.route.context.RouteContext;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.segment.dml.item.ProjectionsSegment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Projections token generator.
 */
@Setter
public final class ProjectionTokenGenerator implements CollectionSQLTokenGenerator<SelectStatementContext>, RouteContextAware {

    private final ShardingExtraRule rule;
    private RouteContext routeContext;

    public ProjectionTokenGenerator(ShardingExtraRule rule) {
        this.rule = rule;
    }

    @Override
    public boolean isGenerateSQLToken(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectStatementContext && !getDerivedProjectionTexts((SelectStatementContext) sqlStatementContext).isEmpty();
    }

    private Collection<? extends SQLToken> getDerivedProjectionTexts(final SelectStatementContext selectStatementContext) {
        ProjectionsSegment projections = selectStatementContext.getSqlStatement().getProjections();
        Map<RouteUnit, String> projection = new HashMap<>();
        for (RouteUnit routeUnit : routeContext.getRouteUnits()) {
            String singleActualTable = ColumnSegments.getSingleActualTable(selectStatementContext, routeUnit);
            String singleLogicTable = ColumnSegments.getSingleLogicTable(selectStatementContext);
            Map<String, String> logicToActual = rule.getLogicToActual(singleLogicTable, singleActualTable);
            ArrayList<String> extract = ColumnSegments.projection(projections, logicToActual);
            StringJoiner sb = new StringJoiner(",");
            for (String segment : extract) {
                if (!segment.isEmpty()) {
                    sb.add(segment);
                }
            }
            projection.put(routeUnit, sb.toString());
        }
        return Collections.singletonList(new ProjectionsToken(projections.getStartIndex(), projections.getStopIndex(), projection));
    }

    @Override
    public Collection<? extends SQLToken> generateSQLTokens(SelectStatementContext sqlStatementContext) {
        return getDerivedProjectionTexts(sqlStatementContext);
    }
}

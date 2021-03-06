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

package cn.superdata.proxy.infra.merge;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import cn.superdata.proxy.infra.rewrite.NewOrderByTokenGenerator;
import org.apache.shardingsphere.infra.binder.statement.SQLStatementContext;
import org.apache.shardingsphere.infra.binder.statement.dml.SelectStatementContext;
import org.apache.shardingsphere.infra.database.type.DatabaseType;
import org.apache.shardingsphere.infra.executor.sql.execute.result.query.QueryResult;
import org.apache.shardingsphere.infra.merge.engine.merger.ResultMerger;
import org.apache.shardingsphere.infra.merge.result.MergedResult;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.sql.parser.sql.common.constant.OrderDirection;

import java.sql.SQLException;
import java.util.List;

/**
 * DQL result merger for Sharding.
 */
public final class SelectResultMerger implements ResultMerger {

    private final ShardingExtraRule rule;

    public SelectResultMerger(DatabaseType databaseType, ShardingExtraRule rule) {
        this.rule = rule;
    }

    @Override
    public MergedResult merge(final List<QueryResult> queryResults, final SQLStatementContext<?> sqlStatementContext, final ShardingSphereSchema schema) throws SQLException {
        HeaderMerge.MappedQueryResults mappedQueryResults = HeaderMerge.projectionMapping(sqlStatementContext, queryResults, rule);
        OrderDirection orderDirection = new NewOrderByTokenGenerator(rule).getOrderDirection((SelectStatementContext) sqlStatementContext);
        return new SelectMergedResult(queryResults, mappedQueryResults, orderDirection);
    }

}

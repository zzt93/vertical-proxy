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

import lombok.Getter;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.Attachable;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.RouteUnitAware;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.SQLToken;
import org.apache.shardingsphere.infra.rewrite.sql.token.pojo.Substitutable;
import org.apache.shardingsphere.infra.route.context.RouteUnit;
import org.apache.shardingsphere.sql.parser.sql.common.constant.OrderDirection;

import java.util.Map;

/**
 * Projections token.
 */
public final class OrderByToken extends SQLToken implements Substitutable, RouteUnitAware {

    private final Map<RouteUnit, String> m;
    @Getter
    private final OrderDirection orderDirection;
    @Getter
    private final int stopIndex;

    public OrderByToken(final int startIndex, int stopIndex, final Map<RouteUnit, String> m, OrderDirection orderDirection) {
        super(startIndex);
        this.m = m;
        this.stopIndex = stopIndex;
        this.orderDirection = orderDirection;
    }
    
    @Override
    public String toString(final RouteUnit routeUnit) {
        return " ORDER BY " + m.get(routeUnit) + " " + orderDirection.name() + " ";
    }

}

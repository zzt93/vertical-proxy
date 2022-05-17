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

package cn.superdata.proxy.core.rule;

import org.apache.shardingsphere.infra.yaml.config.swapper.YamlConfigurationSwapper;

/**
 * Sharding rule configuration YAML swapper.
 */
public final class ColumnConfigurationYamlSwapper implements YamlConfigurationSwapper<YamlColumnRuleConfiguration, ColumnRuleConfiguration> {
    
    @Override
    public YamlColumnRuleConfiguration swapToYamlConfiguration(final ColumnRuleConfiguration data) {
        YamlColumnRuleConfiguration result = new YamlColumnRuleConfiguration();
        result.setPkName(data.getPkName());
        result.setDataNodesPkName(data.getDataNodesPkName());
        result.setLogicToActual(data.getLogicToActual());
        return result;
    }

    @Override
    public ColumnRuleConfiguration swapToObject(final YamlColumnRuleConfiguration data) {
        ColumnRuleConfiguration result = new ColumnRuleConfiguration();
        result.setPkName(data.getPkName());
        result.setDataNodesPkName(data.getDataNodesPkName());
        result.setLogicToActual(data.getLogicToActual());
        return result;
    }

}

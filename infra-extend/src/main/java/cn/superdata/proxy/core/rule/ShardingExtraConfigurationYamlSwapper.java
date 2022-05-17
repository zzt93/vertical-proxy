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

import org.apache.shardingsphere.infra.yaml.config.swapper.YamlRuleConfigurationSwapper;
import org.apache.shardingsphere.infra.yaml.config.swapper.algorithm.ShardingSphereAlgorithmConfigurationYamlSwapper;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;
import org.apache.shardingsphere.sharding.yaml.config.rule.YamlShardingAutoTableRuleConfiguration;
import org.apache.shardingsphere.sharding.yaml.config.rule.YamlTableRuleConfiguration;
import org.apache.shardingsphere.sharding.yaml.swapper.rule.ShardingAutoTableRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.sharding.yaml.swapper.rule.ShardingTableRuleConfigurationYamlSwapper;
import org.apache.shardingsphere.sharding.yaml.swapper.strategy.KeyGenerateStrategyConfigurationYamlSwapper;
import org.apache.shardingsphere.sharding.yaml.swapper.strategy.ShardingStrategyConfigurationYamlSwapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Sharding rule configuration YAML swapper.
 */
public final class ShardingExtraConfigurationYamlSwapper implements YamlRuleConfigurationSwapper<YamlShardingExtraConfiguration, ShardingExtraConfiguration> {
    
    private final ColumnConfigurationYamlSwapper tableYamlSwapper = new ColumnConfigurationYamlSwapper();

    @Override
    public YamlShardingExtraConfiguration swapToYamlConfiguration(final ShardingExtraConfiguration data) {
        YamlShardingExtraConfiguration result = new YamlShardingExtraConfiguration();
        Map<String, YamlColumnRuleConfiguration> m = new HashMap<>();
        for (Entry<String, ColumnRuleConfiguration> e : data.getTables().entrySet()) {
            m.put(e.getKey(), tableYamlSwapper.swapToYamlConfiguration(e.getValue()));
        }
        result.setTables(m);
        return result;
    }

    @Override
    public ShardingExtraConfiguration swapToObject(final YamlShardingExtraConfiguration yamlConfig) {
        ShardingExtraConfiguration result = new ShardingExtraConfiguration();
        Map<String, ColumnRuleConfiguration> m = new HashMap<>();
        for (Entry<String, YamlColumnRuleConfiguration> e : yamlConfig.getTables().entrySet()) {
            m.put(e.getKey(), tableYamlSwapper.swapToObject(e.getValue()));
        }
        result.setTables(m);
        return result;
    }

    @Override
    public Class<ShardingExtraConfiguration> getTypeClass() {
        return ShardingExtraConfiguration.class;
    }
    
    @Override
    public String getRuleTagName() {
        return "EXTRA";
    }
    
    @Override
    public int getOrder() {
        return ShardingOrder.ALGORITHM_PROVIDER_ORDER+1;
    }
}

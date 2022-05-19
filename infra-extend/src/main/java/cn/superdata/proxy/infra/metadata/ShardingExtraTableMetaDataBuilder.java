package cn.superdata.proxy.infra.metadata;

import cn.superdata.proxy.core.rule.ShardingExtraRule;
import org.apache.shardingsphere.infra.config.properties.ConfigurationPropertyKey;
import org.apache.shardingsphere.infra.metadata.schema.builder.SchemaBuilderMaterials;
import org.apache.shardingsphere.infra.metadata.schema.builder.loader.TableMetaDataLoaderEngine;
import org.apache.shardingsphere.infra.metadata.schema.builder.loader.TableMetaDataLoaderMaterial;
import org.apache.shardingsphere.infra.metadata.schema.builder.spi.RuleBasedTableMetaDataBuilder;
import org.apache.shardingsphere.infra.metadata.schema.builder.util.TableMetaDataUtil;
import org.apache.shardingsphere.infra.metadata.schema.model.ColumnMetaData;
import org.apache.shardingsphere.infra.metadata.schema.model.TableMetaData;
import org.apache.shardingsphere.sharding.constant.ShardingOrder;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ShardingExtraTableMetaDataBuilder implements RuleBasedTableMetaDataBuilder<ShardingExtraRule> {

	@Override
	public Map<String, TableMetaData> load(final Collection<String> tableNames, final ShardingExtraRule rule, final SchemaBuilderMaterials materials) throws SQLException {
		if (tableNames.isEmpty()) {
			return Collections.emptyMap();
		}
		Collection<TableMetaDataLoaderMaterial> tableMetaDataLoaderMaterials = TableMetaDataUtil.getTableMetaDataLoadMaterial(tableNames, materials, true);
		if (tableMetaDataLoaderMaterials.isEmpty()) {
			return Collections.emptyMap();
		}
		Collection<TableMetaData> tableMetaDataList = getTableMeta(materials, tableMetaDataLoaderMaterials, rule);
		boolean isCheckingMetaData = materials.getProps().getValue(ConfigurationPropertyKey.CHECK_TABLE_METADATA_ENABLED);
		if (isCheckingMetaData) {
		}
		return getTableMetaDataMap(tableMetaDataList, rule);
	}

	private Collection<TableMetaData> getTableMeta(SchemaBuilderMaterials materials, Collection<TableMetaDataLoaderMaterial> tableMetaDataLoaderMaterials, ShardingExtraRule rule) throws SQLException {
		Collection<TableMetaData> load = TableMetaDataLoaderEngine.load(tableMetaDataLoaderMaterials, materials.getDatabaseType());
		for (TableMetaData each : load) {
			String logicTable = rule.findLogicTableByActualTable(each.getName()).orElse(each.getName());
			Map<String, String> m = rule.getActualToLogic(logicTable);
			HashMap<String, ColumnMetaData> res = new HashMap<>();
			for (Map.Entry<String, ColumnMetaData> e : each.getColumns().entrySet()) {
				ColumnMetaData value = e.getValue();
				String actualName = m.get(value.getName());
				res.put(actualName, new ColumnMetaData(actualName, value.getDataType(), value.isPrimaryKey(), value.isGenerated(), value.isCaseSensitive()));
			}
			each.getColumns().clear();
			each.getColumns().putAll(res);
		}
		return load;
	}

	private Map<String, TableMetaData> getTableMetaDataMap(final Collection<TableMetaData> tableMetaDataList, final ShardingExtraRule rule) {
		Map<String, TableMetaData> result = new LinkedHashMap<>();
		for (TableMetaData each : tableMetaDataList) {
			String logicTable = rule.findLogicTableByActualTable(each.getName()).orElse(each.getName());
			TableMetaData tableMetaData = result.get(logicTable);
			if (tableMetaData != null) {
				ArrayList<ColumnMetaData> columnMetaData = new ArrayList<>(each.getColumns().values());
				columnMetaData.addAll(tableMetaData.getColumns().values());
				result.put(logicTable, new TableMetaData(each.getName(), columnMetaData, each.getIndexes().values()));
			} else {
				result.put(logicTable, each);
			}
		}
		return result;
	}

	@Override
	public TableMetaData decorate(final String tableName, final TableMetaData tableMetaData, final ShardingExtraRule ShardingExtraRule) {
		return tableMetaData;
	}

	@Override
	public int getOrder() {
		return ShardingOrder.ORDER-1;
	}

	@Override
	public Class<ShardingExtraRule> getTypeClass() {
		return ShardingExtraRule.class;
	}

}
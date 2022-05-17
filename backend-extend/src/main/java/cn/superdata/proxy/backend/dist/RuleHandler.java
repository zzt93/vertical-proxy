package cn.superdata.proxy.backend.dist;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.distsql.parser.statement.rdl.RuleDefinitionStatement;
import org.apache.shardingsphere.infra.config.RuleConfiguration;
import org.apache.shardingsphere.infra.distsql.update.RuleDefinitionCreateUpdater;
import org.apache.shardingsphere.infra.distsql.update.RuleDefinitionUpdater;
import org.apache.shardingsphere.infra.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.infra.metadata.schema.ShardingSphereSchema;
import org.apache.shardingsphere.infra.metadata.schema.loader.SchemaLoader;
import org.apache.shardingsphere.infra.rule.ShardingSphereRule;
import org.apache.shardingsphere.infra.rule.builder.schema.SchemaRulesBuilder;
import org.apache.shardingsphere.mode.manager.ContextManager;
import org.apache.shardingsphere.mode.metadata.MetaDataContexts;
import org.apache.shardingsphere.mode.metadata.MetaDataContextsBuilder;
import org.apache.shardingsphere.proxy.backend.context.ProxyContext;
import org.apache.shardingsphere.proxy.backend.text.distsql.rdl.rule.RuleDefinitionBackendHandler;
import org.apache.shardingsphere.spi.typed.TypedSPIRegistry;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @see RuleDefinitionBackendHandler
 */
@Slf4j
public class RuleHandler {

	private void updateRule(String schemaName) {
		ShardingSphereMetaData shardingSphereMetaData = ProxyContext.getInstance().getMetaData(schemaName);
//		RuleDefinitionUpdater ruleDefinitionUpdater = TypedSPIRegistry.getRegisteredService(RuleDefinitionUpdater.class, sqlStatement.getClass().getCanonicalName(), new Properties());
//		processCreate(shardingSphereMetaData, );
		alterRuleConfiguration(ProxyContext.getInstance().getContextManager(), shardingSphereMetaData.getName(), shardingSphereMetaData.getRuleMetaData().getConfigurations());
	}

	public void addRuleConfiguration(ContextManager contextManager, final String schemaName, final Collection<RuleConfiguration> ruleConfigs) {
		MetaDataContexts metaDataContexts = contextManager.getMetaDataContexts();
		ArrayList<RuleConfiguration> all = new ArrayList<>(metaDataContexts.getMetaData(schemaName).getRuleMetaData().getConfigurations());
		all.addAll(ruleConfigs);
		alterRuleConfiguration(contextManager, schemaName, all);
	}

	public void alterRuleConfiguration(ContextManager contextManager, final String schemaName, final Collection<RuleConfiguration> ruleConfigs) {
		try {
			MetaDataContexts metaDataContexts = contextManager.getMetaDataContexts();
			MetaDataContexts changedMetaDataContexts = buildChangedMetaDataContext(metaDataContexts, metaDataContexts.getMetaDataMap().get(schemaName), ruleConfigs);
//			metaDataContexts.getOptimizerContext().getFederationMetaData().getSchemas().putAll(changedMetaDataContexts.getOptimizerContext().getFederationMetaData().getSchemas());
			Map<String, ShardingSphereMetaData> metaDataMap = new HashMap<>(metaDataContexts.getMetaDataMap());
			metaDataMap.putAll(changedMetaDataContexts.getMetaDataMap());
			contextManager.renewMetaDataContexts(rebuildMetaDataContexts(metaDataContexts, metaDataMap));
		} catch (final SQLException ex) {
			log.error("Alter schema:{} rule configuration failed", schemaName, ex);
		}
	}

	private MetaDataContexts buildChangedMetaDataContext(MetaDataContexts metaDataContexts, final ShardingSphereMetaData originalMetaData, final Collection<RuleConfiguration> ruleConfigs) throws SQLException {
		Map<String, Map<String, DataSource>> dataSourcesMap = Collections.singletonMap(originalMetaData.getName(), originalMetaData.getResource().getDataSources());
		Map<String, Collection<RuleConfiguration>> schemaRuleConfigs = Collections.singletonMap(originalMetaData.getName(), ruleConfigs);
		Properties props = metaDataContexts.getProps().getProps();
		Map<String, Collection<ShardingSphereRule>> rules = SchemaRulesBuilder.buildRules(dataSourcesMap, schemaRuleConfigs, props);
		// TODO: 2022/5/13
		Map<String, ShardingSphereSchema> schemas = new SchemaLoader(dataSourcesMap, schemaRuleConfigs, rules, props).load();
		metaDataContexts.getMetaDataPersistService().ifPresent(optional -> optional.getSchemaMetaDataService().persist(originalMetaData.getName(), schemas.get(originalMetaData.getName())));
		return new MetaDataContextsBuilder(dataSourcesMap, schemaRuleConfigs, metaDataContexts.getGlobalRuleMetaData().getConfigurations(), schemas, rules, props)
				.build(metaDataContexts.getMetaDataPersistService().orElse(null));
	}

	private MetaDataContexts rebuildMetaDataContexts(MetaDataContexts metaDataContexts, final Map<String, ShardingSphereMetaData> schemaMetaData) {
		return new MetaDataContexts(metaDataContexts.getMetaDataPersistService().orElse(null),
				schemaMetaData, metaDataContexts.getGlobalRuleMetaData(), metaDataContexts.getExecutorEngine(),
				metaDataContexts.getProps(), metaDataContexts.getOptimizerContext());
	}

	private <T extends RuleDefinitionStatement> void processCreate(final ShardingSphereMetaData shardingSphereMetaData, final T sqlStatement, final RuleDefinitionCreateUpdater updater, final RuleConfiguration currentRuleConfig) {
		RuleConfiguration toBeCreatedRuleConfig = updater.buildToBeCreatedRuleConfiguration(sqlStatement);
		if (null == currentRuleConfig) {
			shardingSphereMetaData.getRuleMetaData().getConfigurations().add(toBeCreatedRuleConfig);
		} else {
			updater.updateCurrentRuleConfiguration(currentRuleConfig, toBeCreatedRuleConfig);
		}
	}
}

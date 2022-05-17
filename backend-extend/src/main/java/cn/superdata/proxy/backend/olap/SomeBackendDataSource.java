package cn.superdata.proxy.backend.olap;

import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.proxy.backend.communication.BackendDataSource;

import java.sql.Connection;
import java.util.List;

public class SomeBackendDataSource implements BackendDataSource {
	@Override
	public List<Connection> getConnections(String schemaName, String dataSourceName, int connectionSize, ConnectionMode connectionMode) {
		return null;
	}

	@Override
	public Connection getConnection(String schema, String format) {
		return null;
	}
}

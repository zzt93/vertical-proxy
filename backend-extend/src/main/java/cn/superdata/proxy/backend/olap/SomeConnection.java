package cn.superdata.proxy.backend.olap;

import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.ExecutorDriverManager;
import org.apache.shardingsphere.infra.executor.sql.prepare.driver.jdbc.StatementOption;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class SomeConnection implements ExecutorDriverManager<Connection, Statement, StatementOption> {
	@Override
	public List<Connection> getConnections(String dataSourceName, int connectionSize, ConnectionMode connectionMode) throws SQLException {
		return null;
	}

	@Override
	public Statement createStorageResource(Connection connection, ConnectionMode connectionMode, StatementOption option) throws SQLException {
		return null;
	}

	@Override
	public Statement createStorageResource(String sql, List<Object> parameters, Connection connection, ConnectionMode connectionMode, StatementOption option) throws SQLException {
		return null;
	}
}

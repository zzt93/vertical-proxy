package cn.superdata.proxy.backend.olap;

import org.apache.shardingsphere.infra.executor.sql.context.ExecutionUnit;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.ConnectionMode;
import org.apache.shardingsphere.infra.executor.sql.execute.engine.SQLExecutionUnit;

public class SomeExecutionUnit implements SQLExecutionUnit {
	@Override
	public ExecutionUnit getExecutionUnit() {
		return null;
	}

	@Override
	public ConnectionMode getConnectionMode() {
		return null;
	}
}

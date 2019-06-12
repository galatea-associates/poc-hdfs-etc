package org.galatea.hadooppoc.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class HBaseAccessor implements AutoCloseable {

	private static final String ZOOKEEPER_CLIENT_PORT = "2181";
	private static final String ZOOKEEPER_QUORUM = "ec2-18-222-172-50.us-east-2.compute.amazonaws.com";
	private static final String HBASE_MASTER_HOSTNAME = "ec2-18-222-172-50.us-east-2.compute.amazonaws.com";
	private static final String HBASE_MASTER_PORT = "16000";
	private static final String ZOOKEEPER_ZNODE_PARENT = "/hbase-unsecure";

	private Configuration configuration;
	private Connection connection;

	@SneakyThrows
	private HBaseAccessor(final Configuration configuration) {
		this.configuration = configuration;
		connection = ConnectionFactory.createConnection(this.configuration);

	}

	@SneakyThrows
	public static HBaseAccessor newDefaultAccessor() {
		return new HBaseAccessor(getDefaultInitializedConfiguration());
	}

	private static Configuration getDefaultInitializedConfiguration() {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENT_PORT);
		configuration.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
		configuration.set("hbase.master.hostname", HBASE_MASTER_HOSTNAME);
		configuration.set("hbase.master.port", HBASE_MASTER_PORT);
		// configuration.set("zookeeper.znode.parent", ZOOKEEPER_ZNODE_PARENT);
		configuration.set("hbase.client.retries.number", Integer.toString(1));
		configuration.set("zookeeper.session.timeout", Integer.toString(60000));
		configuration.set("zookeeper.recovery.retry", Integer.toString(1));
		return configuration;
	}

	@SneakyThrows
	public void checkHbaseAvailable() {
		HBaseAdmin.checkHBaseAvailable(configuration);
	}

	@SneakyThrows
	public void createTable(final HTableDescriptor tableDescriptor) {
		Admin admin = connection.getAdmin();
		log.info("Starting table creation for {}", tableDescriptor.getNameAsString());
		admin.createTable(tableDescriptor);
		admin.close();
	}

	@SneakyThrows
	public void deleteTable(final TableName tableName) {
		Admin admin = connection.getAdmin();
		log.info("Starting table deletion for {}", tableName.getNameAsString());
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		admin.close();
	}

	@SneakyThrows
	public void putTable(final String tableName, final Put put) {
		log.info("Starting table put for {}", tableName);
		Table table = connection.getTable(TableName.valueOf(tableName));
		table.put(put);
		table.close();
	}

	@Override
	public void close() throws Exception {
		connection.close();

	}

}

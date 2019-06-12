package org.galatea.hadooppoc.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import lombok.Getter;
import lombok.SneakyThrows;

@Getter
public class FileSystemFactory {

	private static final String DEFAULT_FS = "hdfs://ec2-18-222-172-50.us-east-2.compute.amazonaws.com:9000";

	private FileSystemFactory(final String defaultFs) {

	}

	@SneakyThrows
	public static FileSystem newDefaultFileSystem() {
		return FileSystem.get(getDefaultConfiguration());
	}

	private static Configuration getDefaultConfiguration() {
		Configuration configuration = new Configuration();
		configuration.set("fs.defaultFS", DEFAULT_FS);
		configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
		configuration.set("fs.file.impl", LocalFileSystem.class.getName());
		configuration.setBoolean("dfs.support.append", true);
		configuration.setBoolean("dfs.client.use.datanode.hostname", true);
		configuration.set("dfs.replication", "1");
		configuration.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", false);
		configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		return configuration;
	}
}

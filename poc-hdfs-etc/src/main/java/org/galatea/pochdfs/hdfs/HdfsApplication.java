package org.galatea.pochdfs.hdfs;

import java.io.File;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsApplication {

	@SneakyThrows
	public static void main(final String[] args) {
		System.setProperty("hadoop.home.dir", "/");
		UpstreamDataManager manager = UpstreamDataManager
				.newManager(new FileWriter(FileSystemFactory.newDefaultFileSystem()));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/swap_data_test/cashFlows.json"));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/swap_data_test/positions.json"));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/swap_data_test/counterParties.json"));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/swap_data_test/instruments.json"));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/swap_data_test/swapContracts.json"));

	}

}

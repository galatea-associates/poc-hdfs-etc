package org.galatea.pochdfs.hdfs;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsApplication {

	@SneakyThrows
	public static void main(final String[] args) {

		FileSystem fs = FileSystemFactory.newDefaultFileSystem();
		UpstreamDataManager manager = UpstreamDataManager.newManager(FileWriter.newFileWriter(fs));

		// for (int i = 0; i < 100; i++) {
		// manager.writeData(new
		// File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/instruments.json"));
		manager.writeData(new File("/Users/kylepayne/Documents/Work/Hadoop_Data/counterparties.json"));
		// manager.writeData(new
		// File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/swapHeader.json"));
		// manager.writeData(new
		// File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/legalEntity.json"));
		// manager.writeData(new
		// File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/positions.json"));
		// manager.writeData(new
		// File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/cashFlows.json"));
		// }
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/instruments.json"));
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/counterparty.json"));
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/swapHeader.json"));
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/legalEntity.json"));
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/positions.json"));
//		manager.writeData(new File("C://Users/kpayne/Documents/Hadoop_AWS/swap_data_test/cashFlows.json"));
	}

}

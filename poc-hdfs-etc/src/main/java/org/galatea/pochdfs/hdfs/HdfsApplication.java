package org.galatea.pochdfs.hdfs;

import java.io.File;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsApplication {

	@SneakyThrows
	public static void main(final String[] args) {
		HdfsWriter hdfsWriter = new HdfsWriter(FileSystemFactory.newDefaultFileSystem());
		SwapDataWriter writer = new SwapDataWriter(hdfsWriter);
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\cashflows.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\positions.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\counterparties.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\instruments.json"));
		writer.writeSwapData(new File("C:\\Users\\kpayne\\Documents\\Hadoop_AWS\\swap_data_test\\swapContracts.json"));

	}

}

package org.galatea.pochdfs.datageneration;

import org.apache.hadoop.fs.FileSystem;
import org.galatea.pochdfs.hdfs.FileSystemFactory;
import org.galatea.pochdfs.hdfs.FileWriter;
import org.galatea.pochdfs.hdfs.UpstreamDataManager;
import org.galatea.pochdfs.hdfs.jsonobjects.CounterParties;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataGenerationApplication {

	@SneakyThrows
	public static void main(final String[] args) {
		FileSystem fs = FileSystemFactory.newDefaultFileSystem();
		UpstreamDataManager manager = UpstreamDataManager.newManager(FileWriter.newFileWriter(fs));
		CounterPartiesGenerator generator = new CounterPartiesGenerator();
		CounterParties counterParties = generator.generateCounterParties(1000000);
		manager.writeData(counterParties);
	}
}

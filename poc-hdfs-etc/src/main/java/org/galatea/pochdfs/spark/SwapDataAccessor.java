package org.galatea.pochdfs.spark;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.spark.SwapDataFiles.SwapDataFilename;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SwapDataAccessor implements AutoCloseable {

	private SparkSession sparkSession;
	private Map<SwapDataFilename, Dataset<Row>> datasets;

	private SwapDataAccessor() {
		sparkSession = SparkSession.builder().appName("SwapDataAccessor").getOrCreate();
		datasets = new HashMap<>();
	}

	public static SwapDataAccessor newDataAccessor() {
		return new SwapDataAccessor();
	}

	public void initializeSwapData(final SwapDataFiles files) {
		log.info("Initializing Swap Data for selected files in path {}", files.getPath());
		// for (SwapDataFilename filename : files.getFilenames()) {
//			Dataset<Row> dataset = sparkSession.read().option("multiLine", true)
//					.json(constructJsonFilePath(files.getPath(), filename.getFilename()));

		// jsonDataset.createOrReplaceTempView(filename.getFilename());
		// log.info("Spark Session created view ".concat(filename.getFilename()));

		// dataset.cache(); // lazy
	}

	public void initializeDefaultSwapData() {
		datasets.put(SwapDataFilename.SWAP_HEADER_200, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/swap-header/", SwapDataFilename.SWAP_HEADER_200.getFilename())));
		datasets.put(SwapDataFilename.COUNTER_PARTIES, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/counterparty/", SwapDataFilename.COUNTER_PARTIES.getFilename())));
		datasets.put(SwapDataFilename.INSTRUMENTS, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/instrument/", SwapDataFilename.INSTRUMENTS.getFilename())));
		datasets.put(SwapDataFilename.LEGAL_ENTITY, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/instrument/", SwapDataFilename.LEGAL_ENTITY.getFilename())));
	}

	private String constructJsonFilePath(final String path, final String fileName) {
		StringBuilder builder = new StringBuilder(path);
		return builder.append(fileName).append(".jsonl").toString();
	}

	public Dataset<Row> executeSql(final String sqlCommand) {
		log.info("Executing SQL command {}", sqlCommand);
		long startTime = System.currentTimeMillis();
		Dataset<Row> dataset = sparkSession.sql(sqlCommand);
		log.info("Execution took {} milliseconds", System.currentTimeMillis() - startTime);
		return dataset;
	}

	public void writeDataset(final Dataset<Row> dataset, final String path) {
		log.info("Writing dataset to path {}", path);
		dataset.write().mode(SaveMode.Overwrite).json(path);
	}

	public Dataset<Row> getEnrichedPositions() {
		Dataset<Row> swapHeader = datasets.get(SwapDataFilename.SWAP_HEADER_200);
		swapHeader.show();
		Dataset<Row> counterParties = datasets.get(SwapDataFilename.COUNTER_PARTIES);
		counterParties.show();
		Dataset<Row> instruments = datasets.get(SwapDataFilename.INSTRUMENTS);
		instruments.show();
		log.info("Joining Swap Positions and Swap Contracts");
		long startTime = System.currentTimeMillis();
		Dataset<Row> dataset = counterParties.join(swapHeader,
				counterParties.col("counterPartyId").equalTo(swapHeader.col("counterPartyId")), "inner");
		// dataset = dataset.join(instruments, dataset.)
		log.info("Execution took {} milliseconds", System.currentTimeMillis() - startTime);
		return dataset;
	}

	@Override
	@SneakyThrows
	public void close() {
		sparkSession.close();
	}

}

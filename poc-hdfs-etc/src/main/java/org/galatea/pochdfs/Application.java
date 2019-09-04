package org.galatea.pochdfs;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.cli.MissingOptionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@SpringBootApplication
public class Application implements ApplicationRunner {

	@SneakyThrows
	public static void main(final String[] args) {

//		SpringApplication.run(Application.class, args);
		SparkSession session = SparkSession.builder().appName("SwapDataAnlyzer")
				.config("spark.sql.shuffle.partitions", 24).config("spark.default.parallelism", 24)
				.config("spark.executor.cores", 4).config("spark.driver.cores", 1).config("spark.driver.memory", "4g")
				.config("spark.executor.memory", "4g").getOrCreate();

		FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
		SwapDataAnalyzer
				analyzer = new SwapDataAnalyzer(new SwapDataAccessor(fileSystemAccessor, "/cs/data/"));

		Dataset<Row> resultWithoutCache = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);

		Dataset <Row> resultWithCache = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);
		resultWithCache = resultWithCache.drop("timeStamp").drop("timestamp").drop("time_stamp");



		log.info("Result set has {} records", resultWithCache.count());
		log.info("Wrighting data to file");
		saveDataset(resultWithCache);
		log.info("File complete");

//		while (true) {
//			Thread.sleep(5000);
//		}

	}

	/**
	 * Ensure that server port is passed in as a command line argument.
	 *
	 * @param args command line arguments
	 * @throws MissingOptionException if server port not provided as argument
	 */
	@Override
	@SneakyThrows
	public void run(final ApplicationArguments args) {
		if (!args.containsOption("server.port") && (System.getProperty("server.port") == null)) {
			throw new MissingOptionException("Server port must be set via command line parameter");
		}
	}

	public static void saveDataset(Dataset<Row> dataset) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter("output.csv", false));
		Iterator<Row> iterator = dataset.toLocalIterator();
		while(iterator.hasNext()){
			Row row = iterator.next();
			writer.write(row.mkString(",") + "\n");
		}
		writer.close();
	}



}

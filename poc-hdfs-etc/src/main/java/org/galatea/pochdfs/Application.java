package org.galatea.pochdfs;

import org.apache.commons.cli.MissingOptionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
//@SpringBootApplication
public class Application implements ApplicationRunner {

	@SneakyThrows
	public static void main(final String[] args) {

//		SpringApplication.run(Application.class, args);
		SparkSession session = SparkSession.builder().appName("SwapDataAnlyzer")
				.config("spark.sql.shuffle.partitions", 16).config("spark.default.parallelism", 16)
				.config("spark.executor.cores", 4).config("spark.driver.cores", 1).config("spark.driver.memory", "6g")
				.config("spark.executor.memory", "6g").getOrCreate();

		FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
		SwapDataAnalyzer analyzer = new SwapDataAnalyzer(new SwapDataAccessor(fileSystemAccessor, "/cs/data/"));
		Dataset<Row> result = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);
		result = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);
		result = result.drop("timeStamp").drop("timestamp").drop("time_stamp");
		log.info("Result set has {} records", result.count());
//		// fileSystemAccessor.writeDataset(result, "/result.json");
//		// result.show();

		while (true) {
			Thread.sleep(5000);
		}

		// System.setProperty("hadoop.home.dir", "/");

//		ExecutorService executor = Executors.newFixedThreadPool(20);
//
//		executor.submit(() -> {
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(HdfsAccessConfig.class);
//			SwapDataWriterService service = (SwapDataWriterService) ctx.getBean("SwapDataWriterService");
//			service.writeData("/bigvolume/test_data/instruments.json");
//		});
//
//		executor.submit(() -> {;
//			ApplicationContext ctx = new AnnotationConfigApplicationContext(HdfsAccessConfig.class);
//			SwapDataWriterService service = (SwapDataWriterService) ctx.getBean("SwapDataWriterService");
//			service.writeData("/bigvolume/test_data/counterparties.json");
//		});
//
//		executor.awaitTermination(60, TimeUnit.SECONDS);
//		executor.shutdown();

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

}

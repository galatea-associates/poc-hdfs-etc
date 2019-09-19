package org.galatea.pochdfs;

import com.google.common.base.Joiner;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import org.apache.commons.cli.MissingOptionException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.speedTest.QuerySpeedTester;
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
//    SparkSession session = SparkSession.builder().appName("SwapDataAnlyzer")
//        .config("spark.sql.shuffle.partitions", 80).config("spark.default.parallelism", 12)
//        .config("spark.executor.cores", 6).config("spark.driver.cores", 1)
//        .config("spark.driver.memory", "16g")
//        .config("spark.executor.memory", "4g").config("spark.executor.instances", 24)
//        .config("spark.driver.maxResultSize","1g").getOrCreate();
//
//    boolean searchWithDates = true;
//    if (args[2].toLowerCase().contains("f")) {
//      searchWithDates = false;
//    }
//
//    FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
//    SwapDataAnalyzer analyzer = new SwapDataAnalyzer(
//        new SwapDataAccessor(fileSystemAccessor, "/cs/data/", searchWithDates));
//    Dataset<Row> resultWithoutCache = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);
//    Dataset<Row> resultWithCache = analyzer.getEnrichedPositionsWithUnpaidCash(args[0], args[1]);
//    resultWithCache = resultWithCache.drop("timeStamp").drop("timestamp").drop("time_stamp");

    Dataset<Row> resultWithCache = QuerySpeedTester.runSpeedTest(args[0], args[1]);

    //log.info("Result set has {} records", resultWithCache.count());
    log.info("Wrighting {} {} data to file", args[0], args[1]);
    wrightDatasetToFile(resultWithCache,"poc_benchmarking/query_results/" + args[0] + "-" + args[1] + "-true.csv");
    log.info("File complete");

//    while(true){
//      Thread.sleep(5000);
//    }

  }

  /**
   * Ensure that server port is passed in as a command line argument.
   *
   * @param args command line arguments
   */
  @Override
  @SneakyThrows
  public void run(final ApplicationArguments args) {
    if (!args.containsOption("server.port") && (System.getProperty("server.port") == null)) {
      throw new MissingOptionException("Server port must be set via command line parameter");
    }
  }

  private static void wrightDatasetToFile(Dataset<Row> dataset, String fileName) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, false));
    writer.write(Joiner.on(",").join(dataset.columns()) + "\n");
    Iterator<Row> iterator = dataset.toLocalIterator();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      writer.write(row.mkString(",") + "\n");
    }
    writer.close();
  }


}

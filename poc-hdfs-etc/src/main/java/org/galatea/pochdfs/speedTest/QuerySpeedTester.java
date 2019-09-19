package org.galatea.pochdfs.speedTest;

import com.google.common.base.Joiner;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;

public class QuerySpeedTester {

  public static SpeedTest currentTest;
  private static int numTests = 3;

  private static String fileName = "speedTestOutput.csv";

  private static final int NUM_CORES = 24;
  private static final int NUM_NODES = 8;

  private static final int PARTITIONS = 48;
  private static final int PARALLELISM = 48;




  public static Dataset<Row> runSpeedTest(String book, String date) throws IOException {

    //SpringApplication.run(Application.class, args);
    SparkSession session = SparkSession.builder().appName("SwapDataAnlyzer")
        .config("spark.sql.shuffle.partitions", PARTITIONS).config("spark.default.parallelism", PARALLELISM)
        .config("spark.executor.cores", 6).config("spark.driver.cores", 1)
        .config("spark.driver.memory", "16g")
        .config("spark.executor.memory", "4g").config("spark.executor.instances", NUM_CORES)
        //.config("spark.driver.maxResultSize","1g")
        .getOrCreate();

    FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
    SwapDataAnalyzer analyzer = new SwapDataAnalyzer(
        new SwapDataAccessor(fileSystemAccessor, "/cs/data/", true));
    currentTest = new SpeedTest();
    Dataset<Row> result = analyzer.getEnrichedPositionsWithUnpaidCash(book, date);


    for(int i = 0; i<numTests; i++) {

      currentTest = new SpeedTest();
      currentTest.setDate(date);
      currentTest.setNumNodes(NUM_NODES);
      currentTest.setNumCores(NUM_CORES);
      currentTest.setPartitions(PARTITIONS);
      currentTest.setParallelism(PARALLELISM);

      result = analyzer.getEnrichedPositionsWithUnpaidCash(book, date);
      writeResult(currentTest);
    }
    return result;

  }

  private static void writeResult(SpeedTest speedTest) throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter("poc_benchmarking/query_results/" + fileName, true));
    if(new File("poc_benchmarking/query_results/" + fileName).exists() == false){
      writer.write("# of Nodes,#of Cores,Date,Total Run Time,total book read,cashflow file read,positions file read,instruments file read,swap contract file read,counterparty,listin leaf,pivot,enriched positions creation,unpaid cash creation,join");
    }
    writer.write(Joiner.on(",").join(speedTest.getAllValues()) + "\n");
    writer.close();
  }

  public static SpeedTest addValue(){
    return currentTest;
  }


}

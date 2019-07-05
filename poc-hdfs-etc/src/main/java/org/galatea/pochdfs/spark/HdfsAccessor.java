package org.galatea.pochdfs.spark;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HdfsAccessor implements AutoCloseable {

	private SparkSession sparkSession;

	public HdfsAccessor() {
		Long startTime = System.currentTimeMillis();
		sparkSession = SparkSession.builder().appName("SwapDataAccessor").getOrCreate();
		log.info("Spark Session created in {} ms", System.currentTimeMillis() - startTime);
	}

	public Dataset<Row> getCounterPartySwapContracts(final int counterPartyId) {
		log.info("Reading swapContracts for counter party id [{}] from HDFS into Spark Dataset", counterPartyId);
		Long startTime = System.currentTimeMillis();
		Dataset<Row> swapContracts = sparkSession.read()
				.json("/cs/data/swapcontracts/" + counterPartyId + "-" + "swapContracts.jsonl");
		log.info("SwapContracts HDFS read took {} ms", System.currentTimeMillis() - startTime);
		return swapContracts;
	}

	public Dataset<Row> getPositions(final long swapId, final int effectiveDate) {
		log.info("Reading swap id [{}] positions with effective date [{}] from HDFS into Spark Dataset", swapId,
				effectiveDate);
		Long startTime = System.currentTimeMillis();
		Dataset<Row> positions = sparkSession.read()
				.json("/cs/data/positions/" + swapId + "-" + effectiveDate + "-" + "positions.jsonl");
		log.info("Positions HDFS read took {} ms", System.currentTimeMillis() - startTime);
		return positions;
	}

	public Dataset<Row> getInstruments() {
		log.info("Reading Instruments from HDFS into Spark Dataset");
		Long startTime = System.currentTimeMillis();
		Dataset<Row> instruments = sparkSession.read().json("/cs/data/instrument/instruments.jsonl");
		log.info("Instruments HDFS read took [{}] ms", System.currentTimeMillis() - startTime);
		return instruments;
	}

	public Dataset<Row> getCounterParties() {
		log.info("Reading counterparties from HDFS into Spark Dataset");
		Long startTime = System.currentTimeMillis();
		Dataset<Row> counterparties = sparkSession.read().json("/cs/data/counterparty/counterparties.jsonl");
		log.info("Counterparties HDFS read took [{}] ms", System.currentTimeMillis() - startTime);
		return counterparties;
	}

	public Dataset<Row> getCashFlows(final long swapId) {
		log.info("Reading cashflows for swapId [{}] from HDFS into Spark Dataset", swapId);
		Long startTime = System.currentTimeMillis();
		Dataset<Row> cashFlows = sparkSession.read().json("/cs/data/cashflows/" + swapId + "-cashFlows.jsonl");
		log.info("CashFlows HDFS read took {} ms", System.currentTimeMillis() - startTime);
		return cashFlows;
	}

	public Dataset<Row> createTemplateDataFrame(final StructType structType) {
		return sparkSession.createDataFrame(new ArrayList<>(), structType);
	}

	public void writeDataset(final Dataset<Row> dataset, final String path) {
		log.info("Writing dataset to path {}", path);
		dataset.write().mode(SaveMode.Overwrite).json(path);
	}

	@Override
	public void close() throws Exception {
		sparkSession.close();
	}

}

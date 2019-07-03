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
		sparkSession = SparkSession.builder().appName("SwapDataAccessor").getOrCreate();
	}

	public Dataset<Row> getCounterPartySwapContracts(final int counterPartyId) {
		return sparkSession.read().json("/cs/data/swapcontracts/" + counterPartyId + "-" + "swapContracts.jsonl");
	}

	public Dataset<Row> getPositions(final long swapId, final int effectiveDate) {
		return sparkSession.read().json("/cs/data/positions/" + swapId + "-" + effectiveDate + "-" + "positions.jsonl");
	}

	public Dataset<Row> getInstruments() {
		return sparkSession.read().json("/cs/data/instrument/instruments.jsonl");
	}

	public Dataset<Row> getCounterParties() {
		return sparkSession.read().json("/cs/data/counterparty/counterparties.jsonl");
	}

	public Dataset<Row> getCashFlows(final long swapId) {
		return sparkSession.read().json("/cs/data/cashflows/" + swapId + "-cashFlows.jsonl");
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

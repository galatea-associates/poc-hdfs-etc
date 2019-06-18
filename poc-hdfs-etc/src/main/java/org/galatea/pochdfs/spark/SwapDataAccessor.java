package org.galatea.pochdfs.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

	public void initializeDefaultSwapData() {
		datasets.put(SwapDataFilename.POSITIONS_200_20190429, sparkSession.read().json(
				constructJsonFilePath("/cs/data/positions/", SwapDataFilename.POSITIONS_200_20190429.getFilename())));
		datasets.put(SwapDataFilename.SWAP_HEADER_200, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/swap-header/", SwapDataFilename.SWAP_HEADER_200.getFilename())));
		datasets.put(SwapDataFilename.COUNTER_PARTIES, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/counterparty/", SwapDataFilename.COUNTER_PARTIES.getFilename())));
		datasets.put(SwapDataFilename.LEGAL_ENTITY, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/legal-entity/", SwapDataFilename.LEGAL_ENTITY.getFilename())));
		datasets.put(SwapDataFilename.INSTRUMENTS, sparkSession.read()
				.json(constructJsonFilePath("/cs/data/instrument/", SwapDataFilename.INSTRUMENTS.getFilename())));
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

	public Dataset<Row> getEnrichedPositionsWithUnpaidCash() {
		Dataset<Row> enrichedPositions = getEnrichedPositions();
		Dataset<Row> unpaidCash = getUnpaidCash(enrichedPositions);
		return joinPositionsWithUnpaidCash(enrichedPositions, unpaidCash);
	}

	public Dataset<Row> getEnrichedPositions() {
		Dataset<Row> positions = datasets.get(SwapDataFilename.POSITIONS_200_20190429);
		Dataset<Row> swapHeader = datasets.get(SwapDataFilename.SWAP_HEADER_200)
				.withColumnRenamed("counterPartyId", "counterPartyIdToDrop")
				.withColumnRenamed("swapId", "swapIdToDrop");
		Dataset<Row> counterParties = datasets.get(SwapDataFilename.COUNTER_PARTIES).withColumnRenamed("counterPartyId",
				"counterPartyIdToDrop");
		Dataset<Row> legalEntity = datasets.get(SwapDataFilename.LEGAL_ENTITY);
		Dataset<Row> instruments = datasets.get(SwapDataFilename.INSTRUMENTS).withColumnRenamed("instrumentId",
				"instrumentIdToDrop");

		Dataset<Row> processedDatasetA = swapHeader
				.join(positions, swapHeader.col("swapIdToDrop").equalTo(positions.col("swapId")), "left")
				.drop("counterPartyIdToDrop", "swapIdToDrop");

		Dataset<Row> processedDatasetB = counterParties
				.join(processedDatasetA,
						counterParties.col("counterPartyIdToDrop").equalTo(processedDatasetA.col("counterPartyId")))
				.drop("counterPartyIdToDrop");

		Dataset<Row> processedDatasetC = legalEntity.join(processedDatasetB,
				legalEntity.col("code").equalTo(processedDatasetB.col("accountingArea")));

		return instruments
				.join(processedDatasetC,
						instruments.col("instrumentIdToDrop").equalTo(processedDatasetC.col("instrumentId")))
				.drop("timestamp").drop("instrumentIdToDrop");
	}

	public Dataset<Row> getUnpaidCash(final Dataset<Row> enrichedPositions) {
		List<Row> swapIdsAndCobDates = enrichedPositions.select("swapId", "cobDate").collectAsList();

		Dataset<Row> unpaidCash = sparkSession.createDataFrame(new ArrayList<>(),
				createDatasetSchema("type", "swapId", "instrumentId", "amount"));

		for (Row row : swapIdsAndCobDates) {
			Dataset<Row> cashFlow = getCashFlow(row.getAs("swapId"));

			int cobDate = Integer.valueOf(row.getAs("cobDate"));

			cashFlow = cashFlow.withColumn("effectiveDateInt", cashFlow.col("effectiveDate").cast("int"))
					.withColumn("payDateInt", cashFlow.col("payDate").cast("int"));

			cashFlow = cashFlow
					.filter(cashFlow.col("effectiveDateInt").leq(cobDate).and(cashFlow.col("payDateInt").geq(cobDate)));

			cashFlow = cashFlow.withColumn("amountInt", cashFlow.col("amount").cast("int"));

			cashFlow = cashFlow.groupBy("type", "swapId", "instrumentId").sum("amountInt");

			cashFlow.show();

			cashFlow = cashFlow.withColumn("amount", cashFlow.col("sum(amountInt)").cast("string"))
					.drop("sum(amountInt)");

			unpaidCash = unpaidCash.union(cashFlow);
		}
		return unpaidCash;
	}

	private StructType createDatasetSchema(final String... fields) {
		List<StructField> structFields = new ArrayList<>();

		for (String field : fields) {
			structFields.add(DataTypes.createStructField(field, DataTypes.StringType, true));
		}

		return DataTypes.createStructType(structFields);
	}

	private Dataset<Row> getCashFlow(final String swapId) {
		return sparkSession.read().json(constructJsonFilePath("/cs/data/cash-flows/", swapId + "-" + "cashFlows"));
	}

	private Dataset<Row> joinPositionsWithUnpaidCash(final Dataset<Row> enrichedPositions,
			final Dataset<Row> unpaidCash) {
		Dataset<Row> renamedUnpaidCash = unpaidCash.withColumnRenamed("swapId", "unpaidCashSwapId")
				.withColumnRenamed("instrumentId", "unpaidCashInstrumentId");
		return enrichedPositions.join(renamedUnpaidCash)
				.where(enrichedPositions.col("swapId").equalTo(renamedUnpaidCash.col("unPaidCashswapId")).and(
						enrichedPositions.col("instrumentId").equalTo(renamedUnpaidCash.col("unpaidCashInstrumentId"))))
				.drop("unpaidCashSwapId", "unpaidCashInstrumentId");
	}

	@Override
	@SneakyThrows
	public void close() {
		sparkSession.close();
	}

}

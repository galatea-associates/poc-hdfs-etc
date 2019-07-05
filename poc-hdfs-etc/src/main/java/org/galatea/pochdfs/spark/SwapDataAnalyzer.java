package org.galatea.pochdfs.spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class SwapDataAnalyzer {

	private HdfsAccessor hdfsAccessor;

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @param effectiveDate  the effective Date
	 * @return a dataset of all enriched positions with unpaid cash for the counter
	 *         party on the effective date
	 */
	public Dataset<Row> getEnrichedPositionsWithUnpaidCash(final int counterPartyId, final int effectiveDate) {
		log.info("Starting Enriched Positions with Unpaid Cash query");
		Long startTime = System.currentTimeMillis();
		Dataset<Row> enrichedPositions = getEnrichedPositions(counterPartyId, effectiveDate);
		Dataset<Row> unpaidCash = getUnpaidCash(counterPartyId, effectiveDate);
		Dataset<Row> enrichedPositionsWithUnpaidCash = joinEnrichedPositionsAndUnpaidCash(enrichedPositions,
				unpaidCash);
		log.info("Completed Enriched Positions with Unpaid Cash query im {} ms",
				System.currentTimeMillis() - startTime);
		return enrichedPositionsWithUnpaidCash;
	}

	/**
	 *
	 * @param enrichedPositions the dataset of enriched positions
	 * @param unpaidCash        the dataset of unpaid cash
	 * @return a dataset of all enriched positions with unpaid cash
	 */
	private Dataset<Row> joinEnrichedPositionsAndUnpaidCash(final Dataset<Row> enrichedPositions,
			final Dataset<Row> unpaidCash) {
		log.info("Joining Enriched Positions with Unpaid Cash");
		Long startTime = System.currentTimeMillis();
		Dataset<Row> dataset = getDatasetWithDroppableColumn(enrichedPositions, "swapId");
		dataset = getDatasetWithDroppableColumn(dataset, "instrumentId");
		dataset = dataset
				.join(unpaidCash,
						dataset.col("droppable-swapId").equalTo(unpaidCash.col("swapId"))
								.and(dataset.col("droppable-instrumentId").equalTo(unpaidCash.col("instrumentId"))))
				.drop("droppable-swapId").drop("droppable-instrumentId");
		log.info("Enriched Positions join with Unpaid Cash finished in {} ms", System.currentTimeMillis() - startTime);
		return dataset;
	}

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @param effectiveDate  the effective date
	 * @return a dataset of all enriched positions for the counter party on the
	 *         effective date
	 */
	public Dataset<Row> getEnrichedPositions(final int counterPartyId, final int effectiveDate) {
		log.info("Getting enriched positions for counter party ID {} on effective date {}", counterPartyId,
				effectiveDate);
		Long startTime = System.currentTimeMillis();
		Dataset<Row> positions = getSwapContractsPositions(getCounterPartySwapIds(counterPartyId), effectiveDate)
				.drop("timeStamp");
		Dataset<Row> swapContracts = hdfsAccessor.getCounterPartySwapContracts(counterPartyId).drop("timeStamp");
		Dataset<Row> counterParties = hdfsAccessor.getCounterParties().drop("timeStamp");
		Dataset<Row> instruments = hdfsAccessor.getInstruments().drop("timeStamp");
		positions = leftJoin(positions, swapContracts, "swapId");
		positions = join(positions, counterParties, "counterPartyId");
		positions = join(positions, instruments, "instrumentId");
		log.info("Completed Enriched Positions query in {} ms", System.currentTimeMillis() - startTime);
		return positions;
	}

	/**
	 *
	 * @param swapIds       the list of swapIds for a counter party
	 * @param effectiveDate the effective date
	 * @return a dataset of all the positions across all swap contracts that a
	 *         specific counter party has for a specific effective date
	 */
	private Dataset<Row> getSwapContractsPositions(final Collection<Long> swapIds, final int effectiveDate) {
		Dataset<Row> totalPositions = hdfsAccessor.createTemplateDataFrame(createPositionsStructType());
		for (Long swapId : swapIds) {
			Dataset<Row> positions = hdfsAccessor.getPositions(swapId, effectiveDate);
			totalPositions = totalPositions.union(positions);
		}
		return totalPositions;
	}

	/**
	 * Creates a struct type with fields in a specific order that match the Dataset
	 * of a positions file. This is used to create an empty datasest template.
	 *
	 * @return a struct type with fields in specific order for positions
	 */
	private StructType createPositionsStructType() {
		List<StructField> structFields = new ArrayList<>();
		structFields.add(DataTypes.createStructField("effectiveDate", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("instrumentId", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("positionType", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("swapId", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("timeStamp", DataTypes.IntegerType, true));
		return DataTypes.createStructType(structFields);
	}

	/**
	 * e.g., SELECT * FROM selectedDataset LEFT JOIN leftJoinedDataset ON
	 * selectedDataset.swapId=leftJoinedDataset.swapId
	 *
	 * @param selectedDataset
	 * @param leftJoinedDataset
	 * @param columnJoinedOn
	 * @return the resulting dataset with the duplicate column dropped
	 */
	private Dataset<Row> leftJoin(final Dataset<Row> selectedDataset, final Dataset<Row> leftJoinedDataset,
			final String columnJoinedOn) {
		Dataset<Row> dataset = getDatasetWithDroppableColumn(selectedDataset, columnJoinedOn);
		return dataset.join(leftJoinedDataset,
				dataset.col("droppable-" + columnJoinedOn).equalTo(leftJoinedDataset.col(columnJoinedOn)), "left")
				.drop("droppable-" + columnJoinedOn);
	}

	/**
	 * e.g., SELECT * FROM firstDataset FULL JOIN secondDataset on
	 * firstDataset.counterPartyId=secondDataset.counterPartyId
	 *
	 * @param firstDataset
	 * @param secondDataset
	 * @param columnJoinOn
	 * @return the resulting dataset with the duplicate column dropped
	 */
	private Dataset<Row> join(final Dataset<Row> firstDataset, final Dataset<Row> secondDataset,
			final String columnJoinedOn) {
		Dataset<Row> dataset = getDatasetWithDroppableColumn(firstDataset, columnJoinedOn);
		return dataset
				.join(secondDataset,
						dataset.col("droppable-" + columnJoinedOn).equalTo(secondDataset.col(columnJoinedOn)))
				.drop("droppable-" + columnJoinedOn);
	}

	/**
	 * Spark does not detect same table columns. This method is used to rename one
	 * of the same columns in order to drop it after a join.
	 *
	 * @param dataset      the dataset with the droppable column
	 * @param columnToDrop the column to drop (e.g., swapId)
	 * @return a dataset with the renamed column (e.g., droppable-swapId)
	 */
	private Dataset<Row> getDatasetWithDroppableColumn(final Dataset<Row> dataset, final String columnToDrop) {
		return dataset.withColumnRenamed(columnToDrop, "droppable-" + columnToDrop);
	}

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @param effectiveDate  the effective date
	 * @return a dataset of all the counterparty's unpaid cash for the specific
	 *         effective date
	 */
	public Dataset<Row> getUnpaidCash(final int counterPartyId, final int effectiveDate) {
		log.info("Getting unpaid cash for counter party ID {} on effective date {}", counterPartyId, effectiveDate);
		Long startTime = System.currentTimeMillis();
		Dataset<Row> unpaidCash = hdfsAccessor.createTemplateDataFrame(createUnpaidCashStructType());
		for (Long swapId : getCounterPartySwapIds(counterPartyId)) {
			Dataset<Row> cashFlows = hdfsAccessor.getCashFlows(swapId);
			Dataset<Row> summedCashFlows = cashFlows.select("instrumentId", "longShort", "amount", "swapId", "type")
					.where(cashFlows.col("effectiveDate").leq(effectiveDate)
							.and(cashFlows.col("payDate").gt(effectiveDate)))
					.groupBy("instrumentId", "longShort", "swapId", "type")
					.agg(functions.sum("amount").as("unpaidCash"));
			unpaidCash = unpaidCash.union(summedCashFlows);
		}
		log.info("Completed Unpaid Cash query in {} ms", System.currentTimeMillis() - startTime);
		return unpaidCash;
	}

	/**
	 * Creates a struct type with fields in a specific order that match the Dataset
	 * of the unpaid cash. This is used to create an empty datasest template.
	 *
	 * @return a struct type with fields in specific order for unpaid cash
	 */
	private StructType createUnpaidCashStructType() {
		List<StructField> structFields = new ArrayList<>();
		structFields.add(DataTypes.createStructField("instrumentId", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("longShort", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("swapId", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("type", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("unpaidCash", DataTypes.DoubleType, true));
		return DataTypes.createStructType(structFields);
	}

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @return a collection of all the swapIds for a specific counter party
	 */
	private Collection<Long> getCounterPartySwapIds(final int counterPartyId) {
		Dataset<Row> swapContracts = hdfsAccessor.getCounterPartySwapContracts(counterPartyId);
		Dataset<Row> swapIdRows = swapContracts.select("swapId")
				.where(swapContracts.col("counterPartyId").equalTo(counterPartyId)).distinct();
		List<Long> swapIds = new ArrayList<>();
		for (Row row : swapIdRows.collectAsList()) {
			swapIds.add((Long) row.getAs("swapId"));
		}
		return swapIds;
	}

}

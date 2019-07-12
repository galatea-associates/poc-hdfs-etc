package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Service;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class SwapDataAnalyzer {

	@Getter
	private final SwapDataAccessor dataAccessor;

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
		debugLogDatasetcontent(enrichedPositionsWithUnpaidCash);
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
		Dataset<Row> dataset = getDatasetWithDroppableColumn(enrichedPositions, "swap_contract_id");
		dataset = getDatasetWithDroppableColumn(dataset, "instrument_id");
		dataset = dataset
				.join(unpaidCash,
						dataset.col("droppable-swap_contract_id").equalTo(unpaidCash.col("swap_contract_id"))
								.and(dataset.col("droppable-instrument_id").equalTo(unpaidCash.col("instrument_id"))))
				.drop("droppable-swap_contract_id").drop("droppable-instrument_id");
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
				.drop("time_stamp");
		Dataset<Row> swapContracts = dataAccessor.getCounterPartySwapContracts(counterPartyId).drop("time_stamp");
		Dataset<Row> counterParties = dataAccessor.getCounterParties().drop("time_stamp");
		Dataset<Row> instruments = dataAccessor.getInstruments().drop("time_stamp");
		positions = leftJoin(positions, swapContracts, "swap_contract_id");
		positions = join(positions, counterParties, "counterparty_id");
		positions = join(positions, instruments, "ric");
		debugLogDatasetcontent(positions);
		log.info("Completed Enriched Positions query in {} ms", System.currentTimeMillis() - startTime);
		return positions;
	}

	private void debugLogDatasetcontent(final Dataset<Row> dataset) {
		log.debug(dataset.schema().toString());
		dataset.foreach((row) -> {
			log.debug(row.toString());
		});
	}

	/**
	 *
	 * @param swapIds       the list of swapIds for a counter party
	 * @param effectiveDate the effective date
	 * @return a dataset of all the positions across all swap contracts that a
	 *         specific counter party has for a specific effective date
	 */
	private Dataset<Row> getSwapContractsPositions(final Collection<Long> swapIds, final int effectiveDate) {
		Dataset<Row> totalPositions = dataAccessor.createTemplateDataFrame(createPositionsStructType());
		for (Long swapId : swapIds) {
			Dataset<Row> positions = dataAccessor.getPositions(swapId, effectiveDate).drop("position_type").distinct();
			totalPositions = totalPositions.union(positions);
		}
		log.debug(String.valueOf(totalPositions.count()));
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
		structFields.add(DataTypes.createStructField("time_stamp", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("ric", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("swap_contract_id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("effective_date", DataTypes.IntegerType, true));
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
		Dataset<Row> unpaidCash = dataAccessor.createTemplateDataFrame(createUnpaidCashStructType());
		for (Long swapId : getCounterPartySwapIds(counterPartyId)) {
			Dataset<Row> cashFlows = dataAccessor.getCashFlows(swapId);
			Dataset<Row> summedCashFlows = cashFlows
					.select("instrument_id", "long_short", "amount", "swap_contract_id", "cashflow_type")
					.where(cashFlows.col("effective_date").leq(effectiveDate)
							.and(cashFlows.col("pay_date").gt(effectiveDate)))
					.groupBy("instrument_id", "long_short", "swap_contract_id", "cashflow_type")
					.agg(functions.sum("amount").as("unpaid_cash"));
			unpaidCash = unpaidCash.union(summedCashFlows);
		}
		unpaidCash = distributeUnpaidCashByType(unpaidCash);
		debugLogDatasetcontent(unpaidCash);
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
		structFields.add(DataTypes.createStructField("instrument_id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("long_short", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("swap_contract_id", DataTypes.IntegerType, true));
		structFields.add(DataTypes.createStructField("cashflow_type", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("unpaid_cash", DataTypes.DoubleType, true));
		return DataTypes.createStructType(structFields);
	}

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @return a collection of all the swapIds for a specific counter party
	 */
	private Collection<Long> getCounterPartySwapIds(final int counterPartyId) {
		Dataset<Row> swapContracts = dataAccessor.getCounterPartySwapContracts(counterPartyId);
		Dataset<Row> swapIdRows = swapContracts.select("swap_contract_id")
				.where(swapContracts.col("counterparty_id").equalTo(counterPartyId)).distinct();
		List<Long> swapIds = new ArrayList<>();
		for (Row row : swapIdRows.collectAsList()) {
			swapIds.add((Long) row.getAs("swap_contract_id"));
		}
		return swapIds;
	}

	/**
	 * Creates an unpaid cash dataset with additional columns that merge the
	 * cashflow type with the unpaid cash
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a dataset including columns for unpaidDiv and unpaidInt
	 */
	private Dataset<Row> distributeUnpaidCashByType(final Dataset<Row> unpaidCash) {
		dataAccessor.createOrReplaceSqlTempView(unpaidCash, "unpaid_cash");
		Dataset<Row> distributedUnpaidCashByType = dataAccessor
				.executeSql("SELECT instrument_id, long_short, swap_contract_id, "
						+ "CASE WHEN cashflow_type=\"DIV\" THEN unpaid_cash ELSE 0 END unpaid_div, CASE WHEN cashflow_type=\"INT\" "
						+ "THEN unpaid_cash ELSE 0 END unpaid_int FROM unpaid_cash");
		distributedUnpaidCashByType = distributedUnpaidCashByType
				.select("instrument_id", "long_short", "swap_contract_id", "unpaid_div", "unpaid_int")
				.groupBy("instrument_id", "long_short", "swap_contract_id")
				.agg(functions.sum("unpaid_div").as("unpaid_div"), functions.sum("unpaid_int").as("unpaid_int"));
		distributedUnpaidCashByType = distributedUnpaidCashByType
				.withColumnRenamed("instrument_id", "droppable-instrument_id")
				.withColumnRenamed("swap_contract_id", "droppable-swap_contract_id");
		distributedUnpaidCashByType = unpaidCash.join(distributedUnpaidCashByType,
				unpaidCash.col("instrument_id").equalTo(distributedUnpaidCashByType.col("droppable-instrument_id"))
						.and(unpaidCash.col("long_short").equalTo(distributedUnpaidCashByType.col("long_short"))
								.and(unpaidCash.col("swap_contract_id")
										.equalTo(distributedUnpaidCashByType.col("droppable-swap_contract_id")))));
		distributedUnpaidCashByType = distributedUnpaidCashByType.drop("droppable-instrument_id").drop("long_short")
				.drop("droppable-swap_contract_id").drop("unpaid_cash").drop("cashflow_type");
		return distributedUnpaidCashByType.distinct();
	}

}

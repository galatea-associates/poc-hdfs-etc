package org.galatea.pochdfs.service.analytics;

import java.util.Collection;
import java.util.Optional;
import java.util.Stack;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.galatea.pochdfs.utils.analytics.DatasetQueryExecutor;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
@Service
public class SwapDataAnalyzer {

	private static final DatasetQueryExecutor QUERY_EXECUTOR = DatasetQueryExecutor.getInstance();

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
		Dataset<Row> dataset = QUERY_EXECUTOR.getDatasetWithDroppableColumn(enrichedPositions, "swap_contract_id");
		dataset = QUERY_EXECUTOR.getDatasetWithDroppableColumn(dataset, "instrument_id");
		dataset = dataset
				.join(unpaidCash,
						dataset.col("droppable-swap_contract_id").equalTo(unpaidCash.col("swap_contract_id"))
								.and(dataset.col("droppable-instrument_id").equalTo(unpaidCash.col("instrument_id"))))
				.drop("droppable-swap_contract_id").drop("droppable-instrument_id");
		log.info("Enriched Positions join with Unpaid Cash finished in {} ms", System.currentTimeMillis() - startTime);
		return dataset;
	}

//	private Dataset<Row> getDatasetWithDroppableColumn(final Dataset<Row> dataset, final String column) {
//		return dataset.withColumnRenamed(column, "droppable-" + column);
//	}

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
		Collection<Long> swapIds = dataAccessor.getCounterPartySwapIds(counterPartyId);
		Optional<Dataset<Row>> positions = getSwapContractsPositions(swapIds, effectiveDate);
		Optional<Dataset<Row>> swapContracts = dataAccessor.getCounterPartySwapContracts(counterPartyId);
		Optional<Dataset<Row>> counterParties = dataAccessor.getCounterParties();
		Optional<Dataset<Row>> instruments = dataAccessor.getInstruments();
		Dataset<Row> enrichedPositions;
		if (datasetsNotEmpty(positions, swapContracts, counterParties, instruments)) {
			enrichedPositions = createEnrichedPositions(positions.get(), swapContracts.get(), counterParties.get(),
					instruments.get());
		} else {
			enrichedPositions = dataAccessor.getBlankDataset();
		}
		log.info("Completed Enriched Positions query in {} ms", System.currentTimeMillis() - startTime);
		debugLogDatasetcontent(enrichedPositions);
		return enrichedPositions;
	}

	/**
	 *
	 * @param swapIds       the list of swapIds for a counter party
	 * @param effectiveDate the effective date
	 * @return a dataset of all the positions across all swap contracts that a
	 *         specific counter party has for a specific effective date
	 */
	private Optional<Dataset<Row>> getSwapContractsPositions(final Collection<Long> swapIds, final int effectiveDate) {
		Stack<Dataset<Row>> totalPositions = new Stack<>();
		for (Long swapId : swapIds) {
			Optional<Dataset<Row>> positions = dataAccessor.getPositions(swapId, effectiveDate);
			if (positions.isPresent()) {
				totalPositions.add(positions.get());
			}
		}
		return combinePositions(totalPositions);
	}

	private Optional<Dataset<Row>> combinePositions(final Stack<Dataset<Row>> positions) {
		if (positions.isEmpty()) {
			return Optional.empty();
		} else {
			Dataset<Row> result = positions.pop();
			while (!positions.isEmpty()) {
				result = result.union(positions.pop().drop("position_type"));
			}
			return Optional.of(result.distinct());
		}
	}

	private Dataset<Row> createEnrichedPositions(final Dataset<Row> positions, final Dataset<Row> swapContracts,
			final Dataset<Row> counterParties, final Dataset<Row> instruments) {
		Dataset<Row> enrichedPositions = QUERY_EXECUTOR.leftJoin(positions, swapContracts, "swap_contract_id");
		enrichedPositions = QUERY_EXECUTOR.join(positions, counterParties, "counterparty_id");
		enrichedPositions = QUERY_EXECUTOR.join(positions, instruments, "ric");
		return enrichedPositions.drop("time_stamp");
	}

	private void debugLogDatasetcontent(final Dataset<Row> dataset) {
		log.debug(dataset.schema().toString());
		dataset.foreach((row) -> {
			log.debug(row.toString());
		});
	}

	public Dataset<Row> getUnpaidCash(final int counterPartyId, final int effectiveDate) {
		log.info("Getting unpaid cash for counter party ID {} on effective date {}", counterPartyId, effectiveDate);
		Long startTime = System.currentTimeMillis();

		Collection<Long> swapIds = dataAccessor.getCounterPartySwapIds(counterPartyId);
		Optional<Dataset<Row>> unpaidCash = getUnpaidCashForContracts(swapIds, counterPartyId, effectiveDate);

		if (datasetsNotEmpty(unpaidCash)) {
			Dataset<Row> distributeUnpaidCash = distributeUnpaidCashByType(unpaidCash.get());
			log.info("Completed Unpaid Cash query in {} ms", System.currentTimeMillis() - startTime);
			return distributeUnpaidCash;
		} else {
			Dataset<Row> blankSet = dataAccessor.getBlankDataset();
			log.info("Completed Unpaid Cash query in {} ms", System.currentTimeMillis() - startTime);
			return blankSet;
		}
	}

	private Optional<Dataset<Row>> getUnpaidCashForContracts(final Collection<Long> swapIds, final int counterPartyId,
			final int effectiveDate) {
		Stack<Dataset<Row>> unpaidCash = new Stack<>();
		for (Long swapId : dataAccessor.getCounterPartySwapIds(counterPartyId)) {
			Optional<Dataset<Row>> cashFlows = dataAccessor.getCashFlows(swapId);
			if (cashFlows.isPresent()) {
				Dataset<Row> data = cashFlows.get();
				Dataset<Row> summedCashFlows = data
						.select("instrument_id", "long_short", "amount", "swap_contract_id", "cashflow_type")
						.where(data.col("effective_date").leq(effectiveDate)
								.and(data.col("pay_date").gt(effectiveDate)))
						.groupBy("instrument_id", "long_short", "swap_contract_id", "cashflow_type")
						.agg(functions.sum("amount").as("unpaid_cash"));
				unpaidCash.add(summedCashFlows);
			}
		}
		return combineUnpaidCash(unpaidCash);
	}

	private Optional<Dataset<Row>> combineUnpaidCash(final Stack<Dataset<Row>> unpaidCash) {
		if (unpaidCash.isEmpty()) {
			return Optional.empty();
		} else {
			Dataset<Row> result = unpaidCash.pop();
			while (!unpaidCash.isEmpty()) {
				result = result.union(unpaidCash.pop());
			}
			return Optional.of(result.distinct());
		}
	}

	private boolean datasetsNotEmpty(final Optional<?>... datasets) {
		for (Optional<?> dataset : datasets) {
			if (!dataset.isPresent()) {
				return false;
			}
		}
		return true;
	}

//	/**
//	 * Creates a struct type with fields in a specific order that match the Dataset
//	 * of the unpaid cash. This is used to create an empty datasest template.
//	 *
//	 * @return a struct type with fields in specific order for unpaid cash
//	 */
////	private StructType createUnpaidCashStructType() {
////		List<StructField> structFields = new ArrayList<>();
////		structFields.add(DataTypes.createStructField("instrument_id", DataTypes.IntegerType, true));
////		structFields.add(DataTypes.createStructField("long_short", DataTypes.StringType, true));
////		structFields.add(DataTypes.createStructField("swap_contract_id", DataTypes.IntegerType, true));
////		structFields.add(DataTypes.createStructField("cashflow_type", DataTypes.StringType, true));
////		structFields.add(DataTypes.createStructField("unpaid_cash", DataTypes.DoubleType, true));
////		return DataTypes.createStructType(structFields);
////	}

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

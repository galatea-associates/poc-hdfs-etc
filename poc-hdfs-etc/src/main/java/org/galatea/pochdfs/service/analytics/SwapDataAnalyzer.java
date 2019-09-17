package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.galatea.pochdfs.domain.analytics.BookSwapDataState;
import org.galatea.pochdfs.utils.analytics.DatasetTransformer;
import org.galatea.pochdfs.utils.analytics.SwapStateGetter;
import org.galatea.pochdfs.speedTest.QuerySpeedTester;

import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Slf4j
public class SwapDataAnalyzer {

	private static final DatasetTransformer	DATASET_TRANSFORMER	= DatasetTransformer.getInstance();

	private final SwapDataAccessor			dataAccessor;

	private final SwapStateGetter			stateGetter;

	public SwapDataAnalyzer(final SwapDataAccessor dataAccessor) {
		this.dataAccessor = dataAccessor;
		stateGetter = new SwapStateGetter(dataAccessor);
	}

	/**
	 *
	 * @param book the book
	 * @param effectiveDate  the effective Date
	 * @return a dataset of all enriched positions with unpaid cash for the counter
	 *         party on the effective date
	 */
	public Dataset<Row> getEnrichedPositionsWithUnpaidCash(final String book, final String effectiveDate) {
		log.info("Starting Enriched Positions with Unpaid Cash query");
		Long startTime = System.currentTimeMillis();

		Long subStartTime = System.currentTimeMillis();
		log.info("Reading in current state");
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);

		log.info("Current state read took {} ms", System.currentTimeMillis() - subStartTime);
		//QuerySpeedTester.addValue().setTotalBookRead(System.currentTimeMillis()-subStartTime);

		subStartTime = System.currentTimeMillis();
		log.info("Creating enriched positions for book {} with effective date {}", book, effectiveDate);
		Dataset<Row> enrichedPositions = getEnrichedPositions(currentState);

		log.info("Completed enriched positions creation in {} ms", System.currentTimeMillis() - subStartTime);
		//QuerySpeedTester.addValue().setEnrichedPositionTime(System.currentTimeMillis()-subStartTime);

		subStartTime = System.currentTimeMillis();
		log.info("Getting unpaid cash for book {} with effective date {}", book, effectiveDate);
		Dataset<Row> unpaidCash = getUnpaidCash(currentState);

		log.info("Completed unpaid cash creation in {} ms", System.currentTimeMillis() - subStartTime);
		//QuerySpeedTester.addValue().setCashCreationTime(System.currentTimeMillis()-subStartTime);

		subStartTime = System.currentTimeMillis();
		log.info("Joining enriched positions with unpaid cash");
		Dataset<Row> enrichedPositionsWithUnpaidCash = joinEnrichedPositionsAndUnpaidCash(enrichedPositions,
				unpaidCash);
		log.info("Completed join in {} ms", System.currentTimeMillis() - subStartTime);
		//QuerySpeedTester.addValue().setJoinTime(System.currentTimeMillis()-subStartTime);

		log.info("Completed Enriched Positions with Unpaid Cash query im {} ms",
				System.currentTimeMillis() - startTime);
		//QuerySpeedTester.addValue().setTotalRunTime(System.currentTimeMillis()-startTime);
		return enrichedPositionsWithUnpaidCash;
	}

	/**
	 *
	 * @param book          the book
	 * @param effectiveDate the effective date
	 * @return a dataset of all enriched positions for the book on the effective
	 *         date
	 */
	public Dataset<Row> getEnrichedPositions(final String book, final String effectiveDate) {
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);

		return getEnrichedPositions(currentState);
	}

	private Dataset<Row> getEnrichedPositions(final BookSwapDataState currentState) {
		if (currentState.positionDataExists()) {
			return createEnrichedPositions(currentState);
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	private Dataset<Row> createEnrichedPositions(final BookSwapDataState currentState) {
		final Dataset<Row> positions = currentState.positions().get();
		final Dataset<Row> swapContracts = currentState.swapContracts().get();
		final Dataset<Row> counterParties = currentState.counterParties().get();
		final Dataset<Row> instruments = currentState.instruments().get();
		Dataset<Row> startOfDatePositions = positions.filter(positions.col("position_type").equalTo("S"));
		Dataset<Row> enrichedPositions = DATASET_TRANSFORMER.leftJoin(startOfDatePositions, swapContracts,
				"swap_contract_id");
		enrichedPositions = DATASET_TRANSFORMER.join(enrichedPositions, counterParties, "counterparty_id");
		enrichedPositions = DATASET_TRANSFORMER.join(enrichedPositions, instruments, "ric");
		return enrichedPositions.drop("time_stamp");
	}

	public Dataset<Row> getUnpaidCash(final String book, final String effectiveDate) {
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);

		return getUnpaidCash(currentState);
	}

	private Dataset<Row> getUnpaidCash(final BookSwapDataState currentState) {
		if (currentState.cashflowDataExists()) {
			return createUnpaidCash(currentState);
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	private Dataset<Row> createUnpaidCash(final BookSwapDataState currentState) {
		Dataset<Row> unpaidCash = calculateUnpaidCash(currentState);
		Dataset<Row> normalizedUnpaidCash = normalizeUnpaidCashByType(unpaidCash);
		return normalizedUnpaidCash;
	}

	/**
	 *
	 * @param currentState the current effective date swap data for the book
	 * @return the summed unpaid cash for the effective date
	 */
	private Dataset<Row> calculateUnpaidCash(final BookSwapDataState currentState) {

		// mitigating for data gen incorrect cash flow effective date format
		String formattedEffectiveDate = currentState.effectiveDate().replaceAll("-", "");

		Dataset<Row> cashFlows = currentState.cashFlows().get();
		Dataset<Row> unpaidCash = cashFlows
				.filter(cashFlows.col("effective_date").leq(functions.lit(currentState.effectiveDate()))
						.and(cashFlows.col("pay_date").gt(functions.lit(currentState.effectiveDate()))));
		unpaidCash = unpaidCash.groupBy("ric", "long_short", "swap_contract_id", "cashflow_type")
				.agg(functions.sum("amount").as("unpaid_cash"));
		return unpaidCash;
	}

	/**
	 * Creates an unpaid cash dataset with additional columns that merge the
	 * cashflow type with the unpaid cash. For example, an unpaid cash result might
	 * have two records for one position because there are two types, INT and DIV.
	 * This method would join those two records into one record by creating
	 * unpaid_INT and unpaid_DIV columns for the position.
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a normalized unpaid cash dataset
	 */
	private Dataset<Row> normalizeUnpaidCashByType(final Dataset<Row> unpaidCash) {
		List<String> cashFlowTypes = getCashFlowTypes(unpaidCash);
		Dataset<Row> distributedUnpaidCash = distributeUnpaidCashByType(unpaidCash, cashFlowTypes);
		Dataset<Row> normalizedUnpaidCash = combineDistributedUnpaidCash(cashFlowTypes, distributedUnpaidCash)
				.distinct();
		return normalizedUnpaidCash;
	}

	/**
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a list of unique cash flow types (e.g., ["INT", "DIV"])
	 */
	private List<String> getCashFlowTypes(final Dataset<Row> unpaidCash) {
		Dataset<Row> cashFlowTypeRows = unpaidCash.select("cashflow_type").dropDuplicates();
		cashFlowTypeRows.cache();

		Dataset<Row> pivotSet = cashFlowTypeRows.withColumn("tempGroupCol", functions.lit(0)).withColumn("tempSumCol",
				functions.lit(0));

		return Arrays.asList(pivotSet.groupBy("tempGroupCol").pivot("cashflow_type").sum("tempSumCol")
				.drop("tempGroupCol").columns());
	}

	/**
	 * Creates the extra unpaid columns for the different cashflow types
	 *
	 * @param unpaidCash    the unpaid cash
	 * @param cashFlowTypes the list of cash flow types
	 * @return unpaid cash with unpaid columns for the different cashflow types
	 */
	private Dataset<Row> distributeUnpaidCashByType(final Dataset<Row> unpaidCash, final List<String> cashFlowTypes) {
		String sqlView = "unpaid_cash";
		dataAccessor.createOrReplaceSqlTempView(unpaidCash, sqlView);
		String distributionQuery = buildUnpaidCashDistributionByTypeQuery(cashFlowTypes, sqlView);
		return dataAccessor.executeSql(distributionQuery);
	}

	private String buildUnpaidCashDistributionByTypeQuery(final Collection<String> cashFlowTypes,
			final String sqlView) {
		StringBuilder builder = new StringBuilder("SELECT * ");
		for (String type : cashFlowTypes) {
			builder.append(", CASE WHEN cashflow_type=\"").append(type).append("\"");
			builder.append(" THEN unpaid_cash ELSE 0 END unpaid_").append(type);
		}
		builder.append(" FROM ").append(sqlView);
		return builder.toString();
	}

	/**
	 * Combines the unpaid cash for each effective date position into one record
	 *
	 * @param cashFlowTypes         the list of cash flow types
	 * @param distributedUnpaidCash the unpaid cash with unpaid columns for the
	 *                              different cashflow types
	 * @return a normalized unpaid cash dataset
	 */
	private Dataset<Row> combineDistributedUnpaidCash(final List<String> cashFlowTypes,
			final Dataset<Row> distributedUnpaidCash) {
		List<String> unpaidCashTypes = new ArrayList<>();
		for (String type : cashFlowTypes) {
			unpaidCashTypes.add("unpaid_" + type);
		}
		Dataset<Row> result = distributedUnpaidCash.groupBy("ric", "long_short", "swap_contract_id")
				.sum(convertListToSeq(unpaidCashTypes));
		return renameSummedTypeColumns(result, cashFlowTypes);
	}

	private Seq<String> convertListToSeq(final List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	private Dataset<Row> renameSummedTypeColumns(final Dataset<Row> unpaidCash,
			final Collection<String> cashFlowTypes) {
		Dataset<Row> result = unpaidCash;
		for (String type : cashFlowTypes) {
			result = result.withColumnRenamed("sum(unpaid_" + type + ")", "unpaid_" + type);
		}
		return result;
	}

	/**
	 *
	 * @param enrichedPositions the dataset of enriched positions
	 * @param unpaidCash        the dataset of unpaid cash
	 * @return a dataset of all enriched positions with unpaid cash
	 */
	private Dataset<Row> joinEnrichedPositionsAndUnpaidCash(final Dataset<Row> enrichedPositions,
			final Dataset<Row> unpaidCash) {
		Dataset<Row> dataset = DATASET_TRANSFORMER.getDatasetWithDroppableColumn(enrichedPositions, "swap_contract_id");
		dataset = DATASET_TRANSFORMER.getDatasetWithDroppableColumn(dataset, "ric");
		dataset = dataset
				.join(unpaidCash,
						dataset.col("droppable-swap_contract_id").equalTo(unpaidCash.col("swap_contract_id"))
								.and(dataset.col("droppable-ric").equalTo(unpaidCash.col("ric"))))
				.drop("droppable-swap_contract_id").drop("droppable-ric").distinct();
		return dataset;
	}


}

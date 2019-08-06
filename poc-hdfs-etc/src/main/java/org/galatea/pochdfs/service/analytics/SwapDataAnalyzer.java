package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.galatea.pochdfs.domain.analytics.BookSwapDataState;
import org.galatea.pochdfs.domain.analytics.SwapStateGetter;
import org.galatea.pochdfs.utils.analytics.DatasetTransformer;
import org.galatea.pochdfs.utils.benchmarking.MethodStats;

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
	 * @param counterPartyId the counter party ID
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

		subStartTime = System.currentTimeMillis();
		log.info("Creating enriched positions for book {} with effective date {}", book, effectiveDate);
		Dataset<Row> enrichedPositions = getEnrichedPositions(currentState);
		log.info("Completed enriched positions creation in {} ms", System.currentTimeMillis() - subStartTime);

		subStartTime = System.currentTimeMillis();
		log.info("Getting unpaid cash for book {} with effective date {}", book, effectiveDate);
		Dataset<Row> unpaidCash = getUnpaidCash(currentState);
		log.info("Completed unpaid cash creation in {} ms", System.currentTimeMillis() - subStartTime);

		subStartTime = System.currentTimeMillis();
		log.info("Joining enriched positions with unpaid cash");
		Dataset<Row> enrichedPositionsWithUnpaidCash = joinEnrichedPositionsAndUnpaidCash(enrichedPositions,
				unpaidCash);
		log.info("Completed join in {} ms", System.currentTimeMillis() - subStartTime);

		log.info("Completed Enriched Positions with Unpaid Cash query im {} ms",
				System.currentTimeMillis() - startTime);
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

	@MethodStats
	private Dataset<Row> getEnrichedPositions(final BookSwapDataState currentState) {
		if (currentState.positionDataExists()) {
			return createEnrichedPositions(currentState);
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	@MethodStats
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

	@MethodStats
	public Dataset<Row> getUnpaidCash(final String book, final String effectiveDate) {
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);
		return getUnpaidCash(currentState);
	}

	@MethodStats
	private Dataset<Row> getUnpaidCash(final BookSwapDataState currentState) {
		if (currentState.cashflowDataExists()) {
			return createUnpaidCash(currentState);
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	@MethodStats
	private Dataset<Row> createUnpaidCash(final BookSwapDataState currentState) {
		Dataset<Row> unpaidCash = calculateUnpaidCash(currentState);
		Dataset<Row> normalizedUnpaidCash = normalizeUnpaidCashByType(unpaidCash);
		normalizedUnpaidCash.show();
		return normalizedUnpaidCash;
	}

	@MethodStats
	private Dataset<Row> calculateUnpaidCash(final BookSwapDataState currentState) {
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
	 * cashflow type with the unpaid cash
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a dataset including columns for unpaidDiv and unpaidInt
	 */
	private Dataset<Row> normalizeUnpaidCashByType(final Dataset<Row> unpaidCash) {
		List<String> cashFlowTypes = getCashFlowTypes(unpaidCash);
		Dataset<Row> distributedUnpaidCash = distributeUnpaidCashByType(unpaidCash, cashFlowTypes);
		Dataset<Row> normalizedUnpaidCash = combineDistributedUnpaidCash(cashFlowTypes, distributedUnpaidCash)
				.distinct();
		return normalizedUnpaidCash;
	}

	@MethodStats
	private List<String> getCashFlowTypes(final Dataset<Row> unpaidCash) {
		Dataset<Row> cashFlowTypeRows = unpaidCash.select("cashflow_type").dropDuplicates();
		cashFlowTypeRows.cache();

		Dataset<Row> pivotSet = cashFlowTypeRows.withColumn("tempGroupCol", functions.lit(0)).withColumn("tempSumCol",
				functions.lit(0));

		return Arrays.asList(pivotSet.groupBy("tempGroupCol").pivot("cashflow_type").sum("tempSumCol")
				.drop("tempGroupCol").columns());

	}

	@MethodStats
	private Dataset<Row> distributeUnpaidCashByType(final Dataset<Row> unpaidCash, final List<String> cashFlowTypes) {
		String sqlView = "unpaid_cash";
		dataAccessor.createOrReplaceSqlTempView(unpaidCash, sqlView);
		String distributionQuery = buildUnpaidCashDistributionByTypeQuery(cashFlowTypes, sqlView);
		return dataAccessor.executeSql(distributionQuery);
	}

	@MethodStats
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

	@MethodStats
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

	@MethodStats
	private Seq<String> convertListToSeq(final List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	@MethodStats
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

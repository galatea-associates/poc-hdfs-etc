package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.galatea.pochdfs.domain.analytics.BookSwapDataState;
import org.galatea.pochdfs.domain.analytics.SwapStateGetter;
import org.galatea.pochdfs.utils.analytics.DatasetTransformer;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Slf4j
public class SwapDataAnalyzer {

	private static final DatasetTransformer	TRANSFORMER	= DatasetTransformer.getInstance();

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

		subStartTime = startTime;
		log.info("Enriched Positions count is {} and unpaid cash count is {}", enrichedPositions.count(),
				unpaidCash.count());

		startTime = subStartTime;

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
	 * @return a dataset of all enriched positions for the counter party on the
	 *         effective date
	 */
	public Dataset<Row> getEnrichedPositions(final String book, final String effectiveDate) {
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);
		return getEnrichedPositions(currentState);
	}

	private Dataset<Row> getEnrichedPositions(final BookSwapDataState currentState) {
		Optional<Dataset<Row>> counterParties = currentState.counterParties();
		Optional<Dataset<Row>> positions = currentState.positions();
		Optional<Dataset<Row>> swapContracts = currentState.swapContracts();
		Optional<Dataset<Row>> instruments = currentState.instruments();
		if (datasetsNotEmpty(positions, swapContracts, counterParties, instruments)) {
			return createEnrichedPositions(positions.get(), swapContracts.get(), counterParties.get(),
					instruments.get());
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	private Dataset<Row> createEnrichedPositions(final Dataset<Row> positions, final Dataset<Row> swapContracts,
			final Dataset<Row> counterParties, final Dataset<Row> instruments) {
		Dataset<Row> startOfDatePositions = positions.filter(positions.col("position_type").equalTo("S"));
		Dataset<Row> enrichedPositions = TRANSFORMER.leftJoin(startOfDatePositions, swapContracts, "swap_contract_id");
		enrichedPositions = TRANSFORMER.join(enrichedPositions, counterParties, "counterparty_id");
		enrichedPositions = TRANSFORMER.join(enrichedPositions, instruments, "ric");
		return enrichedPositions.drop("time_stamp");
	}

	public Dataset<Row> getUnpaidCash(final String book, final String effectiveDate) {
		BookSwapDataState currentState = stateGetter.getBookState(book, effectiveDate);
		return getUnpaidCash(currentState);
	}

	private Dataset<Row> getUnpaidCash(final BookSwapDataState currentState) {
		Optional<Dataset<Row>> unpaidCash = getUnpaidCashForContracts(currentState);

		if (datasetsNotEmpty(unpaidCash)) {
			return distributeUnpaidCashByType(unpaidCash.get());
		} else {
			return dataAccessor.getBlankDataset();
		}
	}

	private Optional<Dataset<Row>> getUnpaidCashForContracts(final BookSwapDataState currentState) {
		Optional<Dataset<Row>> cashflows = currentState.cashFlows();
		if (cashflows.isPresent()) {
			Dataset<Row> unpaidCash = getUnpaidCashFlows(cashflows.get(), currentState.effectiveDate());
			unpaidCash = sumUnpaidCashFlows(unpaidCash);
			return Optional.of(unpaidCash);
		} else {
			return Optional.empty();
		}
	}

	@SneakyThrows
	private Dataset<Row> getUnpaidCashFlows(final Dataset<Row> cashFlows, final String effectiveDate) {
		return cashFlows.select("ric", "long_short", "amount", "swap_contract_id", "cashflow_type")
				.where(cashFlows.col("effective_date").leq(functions.lit(effectiveDate))
						.and(cashFlows.col("pay_date").gt(functions.lit(effectiveDate))));
	}

	private Dataset<Row> sumUnpaidCashFlows(final Dataset<Row> unpaidCash) {
		return unpaidCash.groupBy("ric", "long_short", "swap_contract_id", "cashflow_type")
				.agg(functions.sum("amount").as("unpaid_cash"));
	}

	private boolean datasetsNotEmpty(final Optional<?>... datasets) {
		for (Optional<?> dataset : datasets) {
			if (!dataset.isPresent()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Creates an unpaid cash dataset with additional columns that merge the
	 * cashflow type with the unpaid cash
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a dataset including columns for unpaidDiv and unpaidInt
	 */
	private Dataset<Row> distributeUnpaidCashByType(final Dataset<Row> unpaidCash) {
		List<String> cashFlowTypes = getCashFlowTypes(unpaidCash);
		dataAccessor.createOrReplaceSqlTempView(unpaidCash, "unpaid_cash");

		// Dataset<Row> cashFlowTypes = getCashFlowTypes(unpaidCash);
		Dataset<Row> distributedUnpaidCashByType = dataAccessor
				.executeSql(buildUnpaidCashDistributionQuery(cashFlowTypes));
		distributedUnpaidCashByType = sumDistUnpaidCash(cashFlowTypes, distributedUnpaidCashByType);
		distributedUnpaidCashByType = distributedUnpaidCashByType.withColumnRenamed("ric", "droppable-ric")
				.withColumnRenamed("swap_contract_id", "droppable-swap_contract_id");
		distributedUnpaidCashByType = unpaidCash.join(distributedUnpaidCashByType,
				unpaidCash.col("ric").equalTo(distributedUnpaidCashByType.col("droppable-ric"))
						.and(unpaidCash.col("long_short").equalTo(distributedUnpaidCashByType.col("long_short"))
								.and(unpaidCash.col("swap_contract_id")
										.equalTo(distributedUnpaidCashByType.col("droppable-swap_contract_id")))));
		distributedUnpaidCashByType = distributedUnpaidCashByType.drop("droppable-ric").drop("long_short")
				.drop("droppable-swap_contract_id").drop("unpaid_cash").drop("cashflow_type");
		return distributedUnpaidCashByType.distinct();
	}

	private List<String> getCashFlowTypes(final Dataset<Row> unpaidCash) {
		Dataset<Row> cashFlowTypeRows = unpaidCash.select("cashflow_type").dropDuplicates();
		cashFlowTypeRows.cache();

		// cashFlowTypeRows.coalesce(4);

		Dataset<Row> pivotSet = cashFlowTypeRows.withColumn("tempGroupCol", functions.lit(0)).withColumn("tempSumCol",
				functions.lit(0));

		// unpaidCash.select("cashflow_type").distinct();
//		Iterator<Row> types = cashFlowTypeRows.toLocalIterator();

		return Arrays.asList(pivotSet.groupBy("tempGroupCol").pivot("cashflow_type").sum("tempSumCol")
				.drop("tempGroupCol").columns());

//		List<String> cashFlowTypes = new ArrayList<>();
//		// cashFlowTypeRows.
//		// cashFlowTypeRows.partitio
//		List<Row> types = cashFlowTypeRows.collectAsList();
//		for (Row row : types) {
//			// while (types.hasNext()) {
//
//			cashFlowTypes.add((String) row.getAs("cashflow_type"));
//		}
//		return cashFlowTypes;
	}

	private String buildUnpaidCashDistributionQuery(final Collection<String> cashFlowTypes) {
		StringBuilder builder = new StringBuilder("SELECT ric, long_short, swap_contract_id");
		for (String type : cashFlowTypes) {
			builder.append(", CASE WHEN cashflow_type=\"").append(type).append("\"");
			builder.append(" THEN unpaid_cash ELSE 0 END unpaid_").append(type);
		}
		builder.append(" FROM unpaid_cash");
		return builder.toString();
	}

	private Dataset<Row> sumDistUnpaidCash(final List<String> cashFlowTypes, final Dataset<Row> distUnpaidCash) {
		List<String> unpaidCashTypes = new ArrayList<>();
		for (String type : cashFlowTypes) {
			unpaidCashTypes.add("unpaid_" + type);
		}
		List<String> selectedColumns = new ArrayList<>();
		selectedColumns.addAll(unpaidCashTypes);
		selectedColumns.add("long_short");
		selectedColumns.add("swap_contract_id");
		Dataset<Row> result = distUnpaidCash.select("ric", convertListToSeq(selectedColumns))
				.groupBy("ric", "long_short", "swap_contract_id").sum(convertListToSeq(unpaidCashTypes));
		return renameSummedCashFlowColumns(result, cashFlowTypes);
	}

	private Seq<String> convertListToSeq(final List<String> inputList) {
		return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
	}

	private Dataset<Row> renameSummedCashFlowColumns(final Dataset<Row> unpaidCash,
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
		Dataset<Row> dataset = TRANSFORMER.getDatasetWithDroppableColumn(enrichedPositions, "swap_contract_id");
		dataset = TRANSFORMER.getDatasetWithDroppableColumn(dataset, "ric");
		dataset = dataset
				.join(unpaidCash,
						dataset.col("droppable-swap_contract_id").equalTo(unpaidCash.col("swap_contract_id"))
								.and(dataset.col("droppable-ric").equalTo(unpaidCash.col("ric"))))
				.drop("droppable-swap_contract_id").drop("droppable-ric").distinct();
		return dataset;
	}

}

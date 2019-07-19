package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.galatea.pochdfs.utils.analytics.DatasetQueryExecutor;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConverters;
import scala.collection.Seq;

@Slf4j
@RequiredArgsConstructor
@Service
public class SwapDataAnalyzer {

	private static final DatasetQueryExecutor	QUERY_EXECUTOR	= DatasetQueryExecutor.getInstance();

	// private static final String

	private final SwapDataAccessor				dataAccessor;

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
		Dataset<Row> enrichedPositions = getEnrichedPositions(book, effectiveDate);
		Dataset<Row> unpaidCash = getUnpaidCash(book, effectiveDate);
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

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @param effectiveDate  the effective date
	 * @return a dataset of all enriched positions for the counter party on the
	 *         effective date
	 */
	public Dataset<Row> getEnrichedPositions(final String book, final String effectiveDate) {
		log.info("Getting enriched positions for book {} on effective date {}", book, effectiveDate);
		Long startTime = System.currentTimeMillis();
		Optional<Dataset<Row>> counterParties = dataAccessor.getCounterParties();
		Long counterPartyId = getCounterPartyId(book, counterParties.get());
		Collection<Long> swapIds = dataAccessor.getCounterPartySwapIds(counterPartyId);
		Optional<Dataset<Row>> positions = dataAccessor.getSwapContractsPositions(swapIds, effectiveDate);
		Optional<Dataset<Row>> swapContracts = dataAccessor.getCounterPartySwapContracts(counterPartyId);
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

	private Dataset<Row> createEnrichedPositions(final Dataset<Row> positions, final Dataset<Row> swapContracts,
			final Dataset<Row> counterParties, final Dataset<Row> instruments) {
		Dataset<Row> startOfDatePositions = positions.select("*").where(positions.col("position_type").equalTo("S"))
				.distinct();
		log.debug("TEST COUNT IS {}", startOfDatePositions.count());
		Dataset<Row> enrichedPositions = QUERY_EXECUTOR.leftJoin(startOfDatePositions, swapContracts,
				"swap_contract_id");
		enrichedPositions = QUERY_EXECUTOR.join(enrichedPositions, counterParties, "counterparty_id");
		enrichedPositions = QUERY_EXECUTOR.join(enrichedPositions, instruments, "ric");
		return enrichedPositions.drop("time_stamp");
	}

	private void debugLogDatasetcontent(final Dataset<Row> dataset) {
		log.debug(dataset.schema().toString());
		dataset.foreach((row) -> {
			log.debug(row.toString());
		});
	}

	public Dataset<Row> getUnpaidCash(final String book, final String effectiveDate) {
		log.info("Getting unpaid cash for book {} on effective date {}", book, effectiveDate);
		Long startTime = System.currentTimeMillis();

		Optional<Dataset<Row>> counterParties = dataAccessor.getCounterParties();
		Long counterPartyId = getCounterPartyId(book, counterParties.get());

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

	private Long getCounterPartyId(final String book, final Dataset<Row> counterParties) {
		Dataset<Row> counterParty = counterParties.select("counterparty_id")
				.where(counterParties.col("book").equalTo(book));
		if (counterParty.count() != 1) {
			return -1L;
		} else {
			return counterParty.collectAsList().get(0).getAs("counterparty_id");
		}
	}

	private Optional<Dataset<Row>> getUnpaidCashForContracts(final Collection<Long> swapIds, final Long counterPartyId,
			final String effectiveDate) {
		Stack<Dataset<Row>> unpaidCash = new Stack<>();
		for (Long swapId : dataAccessor.getCounterPartySwapIds(counterPartyId)) {
			Optional<Dataset<Row>> swapContractCashFlows = dataAccessor.getCashFlows(swapId);
			if (swapContractCashFlows.isPresent()) {
				Dataset<Row> cashFlows = swapContractCashFlows.get();
				Dataset<Row> unpaidCashFlows = getUnpaidCashFlows(cashFlows, effectiveDate);
				if (!unpaidCashFlows.isEmpty()) {
					unpaidCash.add(sumUnpaidCashFlows(unpaidCashFlows));
				}
			}
		}
		return combineUnpaidCashResults(unpaidCash);
	}

	@SneakyThrows
	private Dataset<Row> getUnpaidCashFlows(final Dataset<Row> cashFlows, final String effectiveDate) {
//		Date date = QUERY_DATE_FORMAT.parse(((Integer) effectiveDate).toString());
//		String newFormat = DATABASE_DATE_FORMAT.format(date);

		// cashFlows.filter(condition)

		return cashFlows.select("ric", "long_short", "amount", "swap_contract_id", "cashflow_type")
				.where(cashFlows.col("effective_date").leq(functions.lit(effectiveDate))
						.and(cashFlows.col("pay_date").gt(functions.lit(effectiveDate))));
	}

	private Dataset<Row> sumUnpaidCashFlows(final Dataset<Row> cashFlows) {
		return cashFlows.groupBy("ric", "long_short", "swap_contract_id", "cashflow_type")
				.agg(functions.sum("amount").as("unpaid_cash"));
	}

	private Optional<Dataset<Row>> combineUnpaidCashResults(final Stack<Dataset<Row>> unpaidCash) {
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

	/**
	 * Creates an unpaid cash dataset with additional columns that merge the
	 * cashflow type with the unpaid cash
	 *
	 * @param unpaidCash the unpaid cash
	 * @return a dataset including columns for unpaidDiv and unpaidInt
	 */
	private Dataset<Row> distributeUnpaidCashByType(final Dataset<Row> unpaidCash) {
		dataAccessor.createOrReplaceSqlTempView(unpaidCash, "unpaid_cash");
		List<String> cashFlowTypes = getCashFlowTypes(unpaidCash);
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
		Dataset<Row> cashFlowTypeRows = unpaidCash.select("cashflow_type").distinct();
		List<String> cashFlowTypes = new ArrayList<>();
		for (Row row : cashFlowTypeRows.collectAsList()) {
			cashFlowTypes.add((String) row.getAs("cashflow_type"));
		}
		return cashFlowTypes;
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

}

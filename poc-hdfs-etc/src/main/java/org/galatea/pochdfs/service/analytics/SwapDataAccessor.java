package org.galatea.pochdfs.service.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Stack;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SwapDataAccessor {

	private final FilesystemAccessor accessor;
	private final String baseFilePath;

	public Optional<Dataset<Row>> getCounterPartySwapContracts(final int counterPartyId) {
		Optional<Dataset<Row>> swapContracts = accessor
				.getData(baseFilePath + "swapcontracts/" + counterPartyId + "-" + "swapContracts.jsonl");
		return swapContracts;
	}

	public Optional<Dataset<Row>> getPositions(final long swapId, final int effectiveDate) {
		Optional<Dataset<Row>> positions = accessor
				.getData(baseFilePath + "positions/" + swapId + "-" + effectiveDate + "-" + "positions.jsonl");
		return positions;
	}

	public Optional<Dataset<Row>> getInstruments() {
		Optional<Dataset<Row>> instruments = accessor.getData(baseFilePath + "instrument/instruments.jsonl");
		return instruments;
	}

	public Optional<Dataset<Row>> getCounterParties() {
		Optional<Dataset<Row>> counterparties = accessor.getData(baseFilePath + "counterparty/counterparties.jsonl");
		return counterparties;
	}

	public Optional<Dataset<Row>> getCashFlows(final long swapId) {
		log.info("Reading cashflows for swapId [{}] from HDFS into Spark Dataset", swapId);
		Long startTime = System.currentTimeMillis();
		Optional<Dataset<Row>> cashFlows = accessor.getData(baseFilePath + "cashflows/" + swapId + "-cashFlows.jsonl");
		log.info("CashFlows HDFS read took {} ms", System.currentTimeMillis() - startTime);
		return cashFlows;
	}

	/**
	 *
	 * @param counterPartyId the counter party ID
	 * @return a collection of all the swapIds for a specific counter party
	 */
	public Collection<Long> getCounterPartySwapIds(final int counterPartyId) {
		Optional<Dataset<Row>> swapContracts = getCounterPartySwapContracts(counterPartyId);
		if (!swapContracts.isPresent()) {
			return new ArrayList<>();
		} else {
			return combineCounterPartySwapIds(swapContracts, counterPartyId);
		}
	}

	private Collection<Long> combineCounterPartySwapIds(final Optional<Dataset<Row>> swapContracts,
			final int counterPartyId) {
		Dataset<Row> contracts = swapContracts.get();
		Dataset<Row> swapIdRows = contracts.select("swap_contract_id")
				.where(contracts.col("counterparty_id").equalTo(counterPartyId)).distinct();
		List<Long> swapIds = new ArrayList<>();
		for (Row row : swapIdRows.collectAsList()) {
			swapIds.add((Long) row.getAs("swap_contract_id"));
		}
		return swapIds;
	}

	/**
	 *
	 * @param swapIds       the list of swapIds for a counter party
	 * @param effectiveDate the effective date
	 * @return a dataset of all the positions across all swap contracts that a
	 *         specific counter party has for a specific effective date
	 */
	public Optional<Dataset<Row>> getSwapContractsPositions(final Collection<Long> swapIds, final int effectiveDate) {
		Stack<Dataset<Row>> totalPositions = new Stack<>();
		for (Long swapId : swapIds) {
			Optional<Dataset<Row>> positions = getPositions(swapId, effectiveDate);
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
				result = result.union(positions.pop());
			}
			return Optional.of(result);
		}
	}

	public Dataset<Row> getBlankDataset() {
		return accessor.createTemplateDataFrame(new StructType());
	}

	public void createOrReplaceSqlTempView(final Dataset<Row> dataset, final String viewName) {
		accessor.createOrReplaceSqlTempView(dataset, viewName);
	}

	public Dataset<Row> executeSql(final String command) {
		return accessor.executeSql(command);
	}

}

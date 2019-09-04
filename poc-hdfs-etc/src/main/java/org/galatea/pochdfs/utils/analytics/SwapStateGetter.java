package org.galatea.pochdfs.utils.analytics;

import java.util.Collection;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.galatea.pochdfs.domain.analytics.BookSwapDataState;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SwapStateGetter {

	private final SwapDataAccessor dataAccessor;

	public BookSwapDataState getBookState(final String book, final String effectiveDate) {
		Optional<Dataset<Row>> counterParties = dataAccessor.getCounterParties();
		Long counterPartyId = dataAccessor.getCounterPartyId(book, counterParties.get());
		Collection<Long> swapIds = dataAccessor.getCounterPartySwapIds(counterPartyId);
		Optional<Dataset<Row>> positions = dataAccessor.getSwapContractsPositions(swapIds, effectiveDate);
		Optional<Dataset<Row>> swapContracts = dataAccessor.getCounterPartySwapContracts(counterPartyId);
		Optional<Dataset<Row>> instruments = dataAccessor.getInstruments();
		Optional<Dataset<Row>> cashFlows = getCashFlows(effectiveDate, swapIds);

		return new BookSwapDataState().counterParties(counterParties).counterPartyId(counterPartyId)
				.instruments(instruments).positions(positions).swapContracts(swapContracts).swapIds(swapIds).book(book)
				.effectiveDate(effectiveDate).cashFlows(cashFlows);
	}

	private Optional<Dataset<Row>> getCashFlows(final String effectiveDate, final Collection<Long> swapIds) {
		Optional<Dataset<Row>> counterpartyCashFlows = Optional.empty();
		for (Long swapId : swapIds) {
			Optional<Dataset<Row>> cashFlows = dataAccessor.getCashFlows(effectiveDate, swapId,true);
			if (cashFlows.isPresent()) {
				if (counterpartyCashFlows.isPresent()) {
					Dataset<Row> previousCashFlows = counterpartyCashFlows.get();
					counterpartyCashFlows = Optional.of(previousCashFlows.union(cashFlows.get()));
				} else {
					counterpartyCashFlows = cashFlows;
				}
			}
		}
		return counterpartyCashFlows;
	}

}

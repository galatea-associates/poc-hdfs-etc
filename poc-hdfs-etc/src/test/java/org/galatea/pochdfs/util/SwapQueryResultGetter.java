package org.galatea.pochdfs.util;

import org.galatea.pochdfs.domain.result.EnrichedPositionResult;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.domain.result.EnrichedPositionsWithUnpaidCashResults;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SwapQueryResultGetter {

	private final SwapDataAnalyzer analyzer;

	public UnpaidCashResults getUnpaidCashResults(final int counterPartyId, final int effectiveDate) {
		return new UnpaidCashResults(analyzer.getUnpaidCash(counterPartyId, effectiveDate));
	}

	public EnrichedPositionsResults getEnrichedPositionResults(final int counterPartyId, final int effectiveDate) {
		return new EnrichedPositionsResults(analyzer.getEnrichedPositions(counterPartyId, effectiveDate));
	}

	public EnrichedPositionsWithUnpaidCashResults getEnrichedPositionsWithUnpaidCashResults(final int counterPartyId,
			final int effectiveDate) {
		return new EnrichedPositionsWithUnpaidCashResults(
				analyzer.getEnrichedPositionsWithUnpaidCash(counterPartyId, effectiveDate));
	}

	public EnrichedPositionResult getSingleEnrichedPositionResult(final int counterPartyId, final int effectiveDate) {
		EnrichedPositionsResults results = getEnrichedPositionResults(counterPartyId, effectiveDate);
		return results.getSingleResult();
	}

}

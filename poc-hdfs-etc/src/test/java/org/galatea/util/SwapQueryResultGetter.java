package org.galatea.util;

import org.galatea.domain.result.EnrichedPositionsResults;
import org.galatea.domain.result.EnrichedPositionsWithUnpaidCashResults;
import org.galatea.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SwapQueryResultGetter {

	private final SwapDataAnalyzer analyzer;

	public UnpaidCashResults getUnpaidCashResults(final int counterPartyId, final int effectiveDate) {
		return new UnpaidCashResults(analyzer.getUnpaidCash(counterPartyId, effectiveDate));
	}

	public EnrichedPositionsResults getEnrichedPositionResult(final int counterPartyId, final int effectiveDate) {
		return new EnrichedPositionsResults(analyzer.getEnrichedPositions(counterPartyId, effectiveDate));
	}

	public EnrichedPositionsWithUnpaidCashResults getEnrichedPositionsWithUnpaidCashResults(final int counterPartyId,
			final int effectiveDate) {
		return new EnrichedPositionsWithUnpaidCashResults(
				analyzer.getEnrichedPositionsWithUnpaidCash(counterPartyId, effectiveDate));
	}

}

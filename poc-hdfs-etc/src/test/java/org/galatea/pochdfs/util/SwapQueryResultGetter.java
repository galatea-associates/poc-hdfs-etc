package org.galatea.pochdfs.util;

import org.galatea.pochdfs.domain.result.EnrichedPositionResult;
import org.galatea.pochdfs.domain.result.EnrichedPositionsResults;
import org.galatea.pochdfs.domain.result.EnrichedPositionsWithUnpaidCashResults;
import org.galatea.pochdfs.domain.result.UnpaidCashResult;
import org.galatea.pochdfs.domain.result.UnpaidCashResults;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SwapQueryResultGetter {

	private final SwapDataAnalyzer analyzer;

	public UnpaidCashResults getUnpaidCashResults(final String book, final int effectiveDate) {
		return new UnpaidCashResults(analyzer.getUnpaidCash(book, effectiveDate));
	}

	public EnrichedPositionsResults getEnrichedPositionResults(final String book, final int effectiveDate) {
		return new EnrichedPositionsResults(analyzer.getEnrichedPositions(book, effectiveDate));
	}

	public EnrichedPositionsWithUnpaidCashResults getEnrichedPositionsWithUnpaidCashResults(final String book,
			final int effectiveDate) {
		return new EnrichedPositionsWithUnpaidCashResults(
				analyzer.getEnrichedPositionsWithUnpaidCash(book, effectiveDate));
	}

	public EnrichedPositionResult getSingleEnrichedPositionResult(final String book, final int effectiveDate) {
		EnrichedPositionsResults results = getEnrichedPositionResults(book, effectiveDate);
		return results.getSingleResult();
	}

	public UnpaidCashResult getSingleUnpaidCashResult(final String book, final int effectiveDate) {
		UnpaidCashResults results = getUnpaidCashResults(book, effectiveDate);
		return results.getSingleResult();
	}

}

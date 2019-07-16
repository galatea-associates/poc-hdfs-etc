package org.galatea.pochdfs.domain.result;

import org.apache.spark.sql.Row;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class EnrichedPositionResult {

	private final Row enrichedPositionResult;

}

package org.galatea.pochdfs.domain.analytics;

import java.util.Collection;
import java.util.Optional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Getter
@Setter
@Accessors(fluent = true, chain = true)
public class BookSwapDataState {

	private Optional<Dataset<Row>>	counterParties;
	private Long					counterPartyId;
	private String					book;
	private String					effectiveDate;
	private Optional<Dataset<Row>>	positions;
	private Optional<Dataset<Row>>	swapContracts;
	private Optional<Dataset<Row>>	instruments;
	private Collection<Long>		swapIds;
	private Optional<Dataset<Row>>	cashFlows;

	public boolean positionDataExists() {
		return counterParties.isPresent() && positions.isPresent() && swapContracts.isPresent()
				&& instruments.isPresent();
	}

	public boolean cashflowDataExists() {
		return cashFlows.isPresent();
	}

}

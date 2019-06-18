package org.galatea.pochdfs.hdfs.jsonobjects;

import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CounterParties implements JsonObject {

	Collection<CounterParty> counterParties;

	@Override
	public String getObjectType() {
		return "counterParties";
	}

	@Override
	public Collection<CounterParty> getData() {
		return counterParties;
	}

	public void addCounterParty(final CounterParty counterParty) {
		this.counterParties.add(counterParty);
	}

}

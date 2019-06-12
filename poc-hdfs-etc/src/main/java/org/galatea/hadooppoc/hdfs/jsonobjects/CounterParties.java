package org.galatea.hadooppoc.hdfs.jsonobjects;

import java.util.Collection;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
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

}

@Getter
@Setter
@NoArgsConstructor
class CounterParty {

	private String counterPartyId;
	private String entity;
	private String code;
	// private String timeStamp = LocalDateTime.now().toString();

}
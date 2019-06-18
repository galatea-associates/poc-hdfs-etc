package org.galatea.pochdfs.hdfs.jsonobjects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CounterParty {

	private String counterPartyId;
	private String entity;
	private String counterPartyCode;

}
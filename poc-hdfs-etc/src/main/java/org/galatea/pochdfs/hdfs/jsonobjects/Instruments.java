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
public class Instruments implements JsonObject {

	Collection<Instrument> instruments;

	@Override
	public String getObjectType() {
		return "instruments";
	}

	@Override
	public Collection<Instrument> getData() {
		return instruments;
	}

}

package org.galatea.pochdfs.hdfs.jsonobjects;

import java.time.LocalDateTime;
import java.util.Collection;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
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

@Getter
@Setter
@NoArgsConstructor
class Instrument {

	private String ric;
	private String instrumentId;
	private String timeStamp = LocalDateTime.now().toString();

}
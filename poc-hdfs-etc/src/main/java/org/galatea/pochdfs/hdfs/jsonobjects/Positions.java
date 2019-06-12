package org.galatea.pochdfs.hdfs.jsonobjects;

import java.util.Collection;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Positions implements JsonObject {

	Collection<Position> positions;

	@Override
	public String getObjectType() {
		return "positions";
	}

	@Override
	public Collection<Position> getData() {
		return positions;
	}

}
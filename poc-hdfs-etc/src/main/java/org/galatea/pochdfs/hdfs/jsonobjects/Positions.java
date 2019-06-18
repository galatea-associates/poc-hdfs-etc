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
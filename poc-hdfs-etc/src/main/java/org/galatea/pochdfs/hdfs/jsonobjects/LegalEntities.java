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
public class LegalEntities implements JsonObject {

	Collection<LegalEntity> legalEntities;

	@Override
	public String getObjectType() {
		return "legalEntities";
	}

	@Override
	public Collection<LegalEntity> getData() {
		return legalEntities;
	}

}
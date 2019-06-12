package org.galatea.hadooppoc.hdfs.jsonobjects;

import java.time.LocalDateTime;
import java.util.Collection;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
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

@Getter
@Setter
@NoArgsConstructor
class LegalEntity {

	private String code;
	private String name;
	private String timeStamp = LocalDateTime.now().toString();

}
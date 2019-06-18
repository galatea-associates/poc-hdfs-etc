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
public class CashFlows implements JsonObject {

	Collection<CashFlow> cashFlows;

	@Override
	public String getObjectType() {
		return "cashFlows";
	}

	@Override
	public Collection<CashFlow> getData() {
		return cashFlows;
	}

}

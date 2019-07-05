package org.galatea.pochdfs.hdfs;

import java.util.Map;

@FunctionalInterface
public interface IFilePath {

	/**
	 * @param jsonObject the json object object mapped to a map
	 * @return the HDFS file path of the corresponding object
	 */
	public String getFilePath(final Map<String, Object> jsonObject);

}

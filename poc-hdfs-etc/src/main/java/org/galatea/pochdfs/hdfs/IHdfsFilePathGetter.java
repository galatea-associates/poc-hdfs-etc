package org.galatea.pochdfs.hdfs;

import java.util.Map;

@FunctionalInterface
public interface IHdfsFilePathGetter {

	/**
	 * @param jsonObject the object-mapped json object
	 * @return the HDFS file path of the corresponding json object
	 */
	public String getFilePath(final Map<String, Object> jsonObject);

}

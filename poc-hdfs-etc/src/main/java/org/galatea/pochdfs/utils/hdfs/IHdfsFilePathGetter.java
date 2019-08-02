package org.galatea.pochdfs.utils.hdfs;

import java.util.Map;

import org.springframework.stereotype.Component;

@FunctionalInterface
@Component
public interface IHdfsFilePathGetter {

	/**
	 * @param jsonObject the object-mapped json object
	 * @return the HDFS file path of the corresponding json object
	 */
	public String getFilePath(final Map<String, Object> jsonObject);

}

package org.galatea.hadooppoc.hdfs.jsonobjects;

import java.util.Collection;

public interface JsonObject {

	public String getObjectType();

	public Collection<?> getData();

}

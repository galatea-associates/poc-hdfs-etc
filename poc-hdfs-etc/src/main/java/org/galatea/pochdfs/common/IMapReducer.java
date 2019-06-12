package org.galatea.pochdfs.common;

import org.apache.hadoop.mapreduce.Job;

@FunctionalInterface
public interface IMapReducer {
	
	public void mapReduce(final Job job);

}

package org.galatea.hadooppoc.common;

import org.apache.hadoop.mapreduce.Job;

import lombok.SneakyThrows;

public class MapReducer implements IMapReducer {

	private MapReducer() {
	}

	public static MapReducer newMapReducer() {
		return new MapReducer();
	}

	@Override
	@SneakyThrows
	public void mapReduce(final Job job) {
		job.waitForCompletion(true);
	}

}

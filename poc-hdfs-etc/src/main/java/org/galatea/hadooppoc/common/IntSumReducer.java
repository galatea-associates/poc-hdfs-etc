package org.galatea.hadooppoc.common;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import lombok.SneakyThrows;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@SneakyThrows
	public void reduce(Text key, Iterable<IntWritable> values, Context context) {
		IntWritable result = new IntWritable();
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
package org.galatea.hadooppoc.common;

import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import lombok.SneakyThrows;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable WRITABLE_ONE = new IntWritable(1);
	
	private Text word = new Text();

	@SneakyThrows
	public void map(final Object key, final Text value, final Context context){
		StringTokenizer iterator = new StringTokenizer(value.toString());
		while (iterator.hasMoreTokens()) {
			word.set(iterator.nextToken());
			context.write(word, WRITABLE_ONE);
		}
	}
}
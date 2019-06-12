package org.galatea.hadooppoc.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import lombok.Builder;
import lombok.SneakyThrows;

@Builder
@SuppressWarnings("rawtypes")
public class JobInitializer {

	private Class<?> jarClass;
	private Class<? extends Mapper> mapperClass;
	private Class<? extends Reducer> combinerClass;
	private Class<? extends Reducer> reducerClass;
	private Class<?> outputKeyClass;
	private Class<?> outputValueClass;
	private Path inputPath;
	private Path outputPath;

	public Job makeInitializedJob() {
		Job job = getJobInstance();
		setJobClasses(job);
		setJobInputOutputPaths(job);
		return job;
	}

	@SneakyThrows
	private Job getJobInstance() {
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration, "word count");
		return job;
	}

	private void setJobClasses(final Job job) {
		job.setJarByClass(jarClass);
		job.setMapperClass(mapperClass);
		job.setCombinerClass(combinerClass);
		job.setReducerClass(reducerClass);
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
	}

	@SneakyThrows
	private void setJobInputOutputPaths(final Job job) {
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
	}
}

package org.galatea.pochdfs.livy;

import java.util.ArrayList;
import java.util.List;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class PiJob implements Job<Double>, Function<Integer, Integer>, Function2<Integer, Integer, Integer> {

	private final int samples;

	public PiJob(final int samples) {
		this.samples = samples;
	}

	@Override
	public Double call(final JobContext ctx) throws Exception {
		List<Integer> sampleList = new ArrayList<>();
		for (int i = 0; i < samples; i++) {
			sampleList.add(i + 1);
		}
		return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
	}

	@Override
	public Integer call(final Integer v1) {
		double x = Math.random();
		double y = Math.random();
		return (x * x + y * y < 1) ? 1 : 0;
	}

	@Override
	public Integer call(final Integer v1, final Integer v2) {
		return v1 + v2;
	}
}
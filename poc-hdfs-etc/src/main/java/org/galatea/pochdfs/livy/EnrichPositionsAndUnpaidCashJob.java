package org.galatea.pochdfs.livy;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.galatea.pochdfs.spark.HdfsAccessor;
import org.galatea.pochdfs.spark.SwapDataAnalyzer;

public class EnrichPositionsAndUnpaidCashJob implements Job<Dataset<Row>> {

	@Override
	public Dataset<Row> call(final JobContext jobContext) throws Exception {
		HdfsAccessor accessor = new HdfsAccessor(jobContext.sparkSession());
		SwapDataAnalyzer analyzer = new SwapDataAnalyzer(accessor);
		return analyzer.getEnrichedPositionsWithUnpaidCash(200, 20190103);
	}

}

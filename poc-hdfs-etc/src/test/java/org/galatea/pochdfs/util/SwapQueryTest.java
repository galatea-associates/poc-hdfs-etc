package org.galatea.pochdfs.util;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public abstract class SwapQueryTest extends SharedJavaSparkContext implements Serializable {

	private static final long				serialVersionUID	= 1L;
	private final boolean searchWithDates = true;
	protected static SwapQueryResultGetter	resultGetter;


	@Before
	public void initializeResultGetter() {
		SparkSession session = SparkSession.builder().sparkContext(sc()).config("spark.sql.shuffle.partitions", 1)
				.config("spark.default.parallelism", 1).getOrCreate();
		// SparkSession session = new SparkSession(sc());
		FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
		SwapDataAnalyzer analyzer = new SwapDataAnalyzer(
				new SwapDataAccessor(fileSystemAccessor, SwapDatasetFileManager.getINPUT_BASE_PATH(),searchWithDates));
		resultGetter = new SwapQueryResultGetter(analyzer);
	}

	@BeforeClass
	@AfterClass
	public static void holyHandGrenade() {
		SwapDatasetFileManager.deleteData();
	}

}
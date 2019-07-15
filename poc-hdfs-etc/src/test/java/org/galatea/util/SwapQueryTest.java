package org.galatea.util;

import java.io.Serializable;

import org.apache.spark.sql.SparkSession;
import org.galatea.pochdfs.service.analytics.SwapDataAccessor;
import org.galatea.pochdfs.service.analytics.SwapDataAnalyzer;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;
import org.junit.After;
import org.junit.Before;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public abstract class SwapQueryTest extends SharedJavaSparkContext implements Serializable {

	private static final long serialVersionUID = 1L;
	protected SwapQueryResultGetter resultGetter;

	@Before
	public void init() {
		SwapDatasetFileManager.deleteData();
		SparkSession session = new SparkSession(sc());
		FilesystemAccessor fileSystemAccessor = new FilesystemAccessor(session);
		SwapDataAnalyzer analyzer = new SwapDataAnalyzer(
				new SwapDataAccessor(fileSystemAccessor, SwapDatasetFileManager.getINPUT_BASE_PATH()));
		resultGetter = new SwapQueryResultGetter(analyzer);
	}

	@After
	public void tearDown() {
		SwapDatasetFileManager.deleteData();
	}

}

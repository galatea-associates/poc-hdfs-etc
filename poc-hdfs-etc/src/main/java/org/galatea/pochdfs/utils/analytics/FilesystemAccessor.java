package org.galatea.pochdfs.utils.analytics;

import java.util.ArrayList;
import java.util.Optional;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class FilesystemAccessor {

	private final SparkSession sparkSession;

	public Optional<Dataset<Row>> getData(final String path) {
		try {
			log.info("Reading data from path {} into spark dataset", path);
			return Optional.of(attemptGettingData(path));
		} catch (AnalysisException e) {
			log.info("Error reading data with error message: {}. Returning empty Optional instead", e.getMessage());
			return Optional.empty();
		}
	}

	private Dataset<Row> attemptGettingData(final String path) throws AnalysisException {
		return sparkSession.read().json(path);
	}

	public Dataset<Row> createTemplateDataFrame(final StructType structType) {
		return sparkSession.createDataFrame(new ArrayList<>(), structType);
	}

	public void createOrReplaceSqlTempView(final Dataset<Row> dataset, final String viewName) {
		dataset.createOrReplaceTempView(viewName);
	}

	public Dataset<Row> executeSql(final String command) {
		return sparkSession.sql(command);
	}

}

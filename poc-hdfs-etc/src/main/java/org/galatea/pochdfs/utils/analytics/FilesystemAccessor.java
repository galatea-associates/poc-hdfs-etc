package org.galatea.pochdfs.utils.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
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

	public Optional<Dataset<Row>> getData(final String... paths){
		try {
			log.info("Reading {} files", paths.length);
			return Optional.of(attemptGettingData(paths));
		} catch (AnalysisException e) {
			log.info("Error reading data with error message: {}. Returning empty Optional instead", e.getMessage());
			return Optional.empty();
		}
	}

	public FileStatus[] getStatusArray(String directoryPath) {
		log.info("Status Array Reguested");
		try {
			Path path = new Path(directoryPath);
			FileSystem fs = path.getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
			return fs.listStatus(path);
		}
		catch(IOException e){
			log.info("Error Reading From Directory: " + e.getMessage());
			return new FileStatus[0];
		}
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

	public void writeDataset(final Dataset<Row> dataset, final String path) {
		log.info("Writing dataset to path {}", path);
		dataset.write().mode(SaveMode.Overwrite).json(path);
	}

	private Dataset<Row> attemptGettingData(final String... path) throws AnalysisException {
		long startTime = System.currentTimeMillis();
		Dataset<Row> dataset = sparkSession.read().json(path);
		log.info("{} file(s) read in {} ms", path.length, System.currentTimeMillis() - startTime);
		return dataset;
	}

	private Dataset<Row> attemptGettingData(final String path) throws AnalysisException{
		long startTime = System.currentTimeMillis();
		Dataset<Row> dataset = sparkSession.read().json(path);
		log.info("File read in {} ms ", System.currentTimeMillis()-startTime);
		return dataset;
	}

//	public void test() {
//		sparkSession.par
//	}

}

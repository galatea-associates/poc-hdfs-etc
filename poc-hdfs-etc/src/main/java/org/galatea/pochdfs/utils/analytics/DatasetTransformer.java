package org.galatea.pochdfs.utils.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DatasetTransformer {

	private static final DatasetTransformer INSTANCE = new DatasetTransformer();

	private DatasetTransformer() {

	}

	public static DatasetTransformer getInstance() {
		return INSTANCE;
	}

	/**
	 * e.g., SELECT * FROM selectedDataset LEFT JOIN leftJoinedDataset ON
	 * selectedDataset.swapId=leftJoinedDataset.swapId
	 *
	 * @param selectedDataset
	 * @param leftJoinedDataset
	 * @param columnJoinedOn
	 * @return the resulting dataset with the duplicate column dropped
	 */
	public Dataset<Row> leftJoin(final Dataset<Row> selectedDataset, final Dataset<Row> leftJoinedDataset,
			final String columnJoinedOn) {
		Dataset<Row> dataset = getDatasetWithDroppableColumn(selectedDataset, columnJoinedOn);
		return dataset.join(leftJoinedDataset,
				dataset.col("droppable-" + columnJoinedOn).equalTo(leftJoinedDataset.col(columnJoinedOn)), "left")
				.drop("droppable-" + columnJoinedOn);
	}

	/**
	 * e.g., SELECT * FROM firstDataset FULL JOIN secondDataset on
	 * firstDataset.counterPartyId=secondDataset.counterPartyId
	 *
	 * @param firstDataset
	 * @param secondDataset
	 * @param columnJoinOn
	 * @return the resulting dataset with the duplicate column dropped
	 */
	public Dataset<Row> join(final Dataset<Row> firstDataset, final Dataset<Row> secondDataset,
			final String columnJoinedOn) {
		Dataset<Row> dataset = getDatasetWithDroppableColumn(firstDataset, columnJoinedOn);
		return dataset
				.join(secondDataset,
						dataset.col("droppable-" + columnJoinedOn).equalTo(secondDataset.col(columnJoinedOn)))
				.drop("droppable-" + columnJoinedOn);
	}

	/**
	 * Spark does not detect same table columns. This method is used to rename one
	 * of the same columns in order to drop it after a join.
	 *
	 * @param dataset      the dataset with the droppable column
	 * @param columnToDrop the column to drop (e.g., swapId)
	 * @return a dataset with the renamed column (e.g., droppable-swapId)
	 */
	public Dataset<Row> getDatasetWithDroppableColumn(final Dataset<Row> dataset, final String columnToDrop) {
		return dataset.withColumnRenamed(columnToDrop, "droppable-" + columnToDrop);
	}

}

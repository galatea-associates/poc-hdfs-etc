package org.galatea.pochdfs.service.analytics;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.galatea.pochdfs.speedTest.QuerySpeedTester;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mortbay.log.Log;

@Slf4j
@RequiredArgsConstructor
public class SwapDataAccessor {

  private final FilesystemAccessor accessor;
  private final String baseFilePath;
  private final boolean searchWithDates;

  public Optional<Dataset<Row>> getCounterPartySwapContracts(final Long counterPartyId) {
    long startTime = System.currentTimeMillis();
    Optional<Dataset<Row>> swapContracts = accessor
        .getData(baseFilePath + "swapcontracts/" + counterPartyId + "-" + "swapContracts.jsonl");
    //QuerySpeedTester.addValue().setSwapContractRead(System.currentTimeMillis()-startTime);
    return swapContracts;
  }

  public Optional<Dataset<Row>> getEffectiveDateSwapPositions(final long swapId,
      final String effectiveDate) {
    Optional<Dataset<Row>> positions = getPositions(effectiveDate, swapId);
    if (positions.isPresent()) {
      Dataset<Row> actualPositions = positions.get();
      return Optional.of(actualPositions
          .filter(actualPositions.col("effective_date").equalTo(functions.lit(effectiveDate))));
    } else {
      return positions;
    }
  }

  public Optional<Dataset<Row>> getPositions(final String effectiveDate, final long swapId) {
    Optional<Dataset<Row>> positions = accessor
        .getData(
            baseFilePath + "positions/" + effectiveDate + "-" + swapId + "-" + "positions.jsonl");
    return positions;
  }

  public Optional<Dataset<Row>> getInstruments() {
    long startTime = System.currentTimeMillis();
    Optional<Dataset<Row>> instruments =
        accessor.getData(baseFilePath + "instrument/instruments.jsonl");
    //QuerySpeedTester.addValue().setInstrumentRead(System.currentTimeMillis()-startTime);
    return instruments;
  }

  public Optional<Dataset<Row>> getCounterParties() {
    long startTime = System.currentTimeMillis();
    Optional<Dataset<Row>> counterparties =
        accessor.getData(baseFilePath + "counterparty/counterparties.jsonl");
    //QuerySpeedTester.addValue().setCounterpartyRead(System.currentTimeMillis()-startTime);
    return counterparties;
  }

  public Long getCounterPartyId(final String book, final Dataset<Row> counterParties) {
    Dataset<Row> counterParty = counterParties.select("counterparty_id")
        .where(counterParties.col("book").equalTo(book));
    if (counterParty.isEmpty()) {
      return -1L;
    } else {
      return counterParty.first().getAs("counterparty_id");
    }
  }

  public Optional<Dataset<Row>> getCashFlows(final String queryDate, Collection<Long> swapIds) {
    if (searchWithDates) {

      long startTime = System.currentTimeMillis();
      try {
        String [] paths = getCashFlowFilePathsInRange(queryDate,swapIds);
        log.info("CashFlow FilePaths found in {} ms", System.currentTimeMillis() - startTime);
        //QuerySpeedTester.addValue().setListingLeafTime(System.currentTimeMillis()-startTime);
        startTime= System.currentTimeMillis();
        Optional<Dataset<Row>> dataset = accessor.getData(paths);
        //QuerySpeedTester.addValue().setCashFlowRead(System.currentTimeMillis()-startTime);
        return dataset;
      }catch (DateTimeParseException e){
        Log.info("Incorrectly formatted QueryDate Returning Empty DataSet");
        return Optional.empty();
      }

    } else {
      ArrayList<String> paths = new ArrayList<>();
      for(long id: swapIds){
       paths.add(baseFilePath + "cashflows/" + id + "-cashFlows.jsonl");
      }
      Object[] arr = paths.toArray();
      String [] pathArray = Arrays.copyOf(arr, arr.length, String[].class);
      return accessor.getData(pathArray);
    }
  }

  /**
   * @param counterPartyId the counter party ID
   * @return a collection of all the swapIds for a specific counter party
   */
  public Collection<Long> getCounterPartySwapIds(final Long counterPartyId) {
    Optional<Dataset<Row>> swapContracts = getCounterPartySwapContracts(counterPartyId);
    if (!swapContracts.isPresent()) {
      return new ArrayList<>();
    } else {
      return combineCounterPartySwapIds(swapContracts, counterPartyId);
    }
  }

  private Collection<Long> combineCounterPartySwapIds(final Optional<Dataset<Row>> swapContracts,
      final Long counterPartyId) {
    Dataset<Row> contracts = swapContracts.get();
    Dataset<Row> swapIdRows = contracts.select("swap_contract_id")
        .where(contracts.col("counterparty_id").equalTo(counterPartyId)).distinct();
    // Iterator<Row> idRows = swapIdRows.toLocalIterator();

    swapIdRows.cache();

//		List<Long> swapIds = new ArrayList<>();
//		for (Row row : swapIdRows.collectAsList()) {
//			// while (idRows.hasNext()) {
//			swapIds.add((Long) row.getAs("swap_contract_id"));
//		}
//		return swapIds;

    Dataset<Row> pivotSet =
        swapIdRows.withColumn("tempGroupCol", functions.lit(0)).withColumn("tempSumCol",
            functions.lit(0));

    // unpaidCash.select("cashflow_type").distinct();
//		Iterator<Row> types = cashFlowTypeRows.toLocalIterator();

    List<String> stringList =
        Arrays.asList(pivotSet.groupBy("tempGroupCol").pivot("swap_contract_id")
            .sum("tempSumCol").drop("tempGroupCol").columns());
    return stringList.stream().map(s -> Long.valueOf(s)).collect(Collectors.toList());
  }

  /**
   * @param swapIds the list of swapIds for a counter party
   * @param effectiveDate the effective date
   * @return a dataset of all the positions across all swap contracts that a specific counter party
   *     has for a specific effective date
   */
  public Optional<Dataset<Row>> getSwapContractsPositions(final Collection<Long> swapIds,
      final String effectiveDate) {

//    Stack<Dataset<Row>> totalPositions = new Stack<>();
//    for (Long swapId : swapIds) {
//      Optional<Dataset<Row>> positions = getEffectiveDateSwapPositions(swapId, effectiveDate);
//      if (positions.isPresent()) {
//        totalPositions.add(positions.get());
//      }
//    }
//    return combinePositions(totalPositions);

    ArrayList<String> paths = new ArrayList<>();
    for(long swapId: swapIds){
      String path = baseFilePath + "positions/" + effectiveDate + "-" + swapId + "-" + "positions.jsonl";
      log.info("Adding item to positions path list: {}", path);
      paths.add(path);
    }
    Object[] arr = paths.toArray();
    long startTime = System.currentTimeMillis();
    Optional<Dataset<Row>> positions = accessor.getData(Arrays.copyOf(arr, arr.length, String[].class));
    //QuerySpeedTester.addValue().setPositionsRead(System.currentTimeMillis()-startTime);
    return positions;

  }

  public Dataset<Row> getBlankDataset() {
    return accessor.createTemplateDataFrame(new StructType());
  }

  public void createOrReplaceSqlTempView(final Dataset<Row> dataset, final String viewName) {
    accessor.createOrReplaceSqlTempView(dataset, viewName);
  }

  public Dataset<Row> executeSql(final String command) {
    return accessor.executeSql(command);
  }

  private Optional<Dataset<Row>> combinePositions(final Stack<Dataset<Row>> positions) {
    if (positions.isEmpty()) {
      return Optional.empty();
    } else {
      Dataset<Row> result = positions.pop();
      while (!positions.isEmpty()) {
        result = result.union(positions.pop());
      }
      return Optional.of(result);
    }
  }

  private String[] getCashFlowFilePathsInRange(String queryDate, Collection<Long> swapIds) throws DateTimeParseException{

    FileStatus[] status = accessor.getStatusArray(baseFilePath + "/cashflows");
    ArrayList<String> fileNames = new ArrayList<>();
    YearMonth testDate = getQueryYearDate(queryDate);
    for (int i = 0; i < status.length; i++) {
      String fileName = status[i].getPath().getName();
      if (isNewerCashflowsFileVersion(fileName)) {
        if (isValidCashflowsDateRange(testDate, swapIds, status[i].getPath().getName())) {
          fileNames.add(status[i].getPath().toString());
          log.info("Adding item to cashflows path list: {} ", status[i].getPath().toString());
        }
      }
    }
    Object[] arr = fileNames.toArray();
    return Arrays.copyOf(arr, arr.length, String[].class);
  }

  private boolean isNewerCashflowsFileVersion(String fileName) {
    return !fileName.substring(0, 4).contains("-");
  }

  private YearMonth getQueryYearDate(String queryDate) throws DateTimeParseException {
			DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			return YearMonth.from(LocalDate.parse(queryDate, dateFormat));
  }

  private boolean isValidCashflowsDateRange(YearMonth testDate, Collection<Long> swapIds, String path) {
    String name = path.replace("cashflows/", "");
    String[] components = name.split("-");

    DateTimeFormatter yearMonthFormat = DateTimeFormatter.ofPattern("yyyyMM");

    YearMonth effectiveDate = YearMonth.parse(components[0], yearMonthFormat);
    YearMonth payDate = YearMonth.parse(components[1], yearMonthFormat);
    Long pathId = Long.parseLong(components[2]);

    if (!swapIds.contains(pathId)) {
      return false;
    }
    if (effectiveDate.isAfter(testDate) || payDate.isBefore(testDate)) {
      return false;
    }
    return true;
  }

}



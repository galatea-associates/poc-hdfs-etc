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
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.galatea.pochdfs.utils.analytics.FilesystemAccessor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.mortbay.log.Log;

@Slf4j
@RequiredArgsConstructor
public class SwapDataAccessor {

  private final FilesystemAccessor accessor;
  private final String baseFilePath;

  public Optional<Dataset<Row>> getCounterPartySwapContracts(final Long counterPartyId) {
    return accessor
        .getData(baseFilePath + "swapcontracts/" + counterPartyId + "-" + "swapContracts.jsonl");
  }

  public Optional<Dataset<Row>> getInstruments() {
    return accessor.getData(baseFilePath + "instrument/instruments.jsonl");
  }

  public Optional<Dataset<Row>> getCounterParties() {
    return accessor.getData(baseFilePath + "counterparty/counterparties.jsonl");
  }

  public Optional<Dataset<Row>> getCashFlows(final String queryDate, Collection<Long> swapIds) {
    long startTime = System.currentTimeMillis();
    try {
      String[] paths = getCashFlowFilePathsInRange(queryDate, swapIds);
      log.info("CashFlow FilePaths found in {} ms", System.currentTimeMillis() - startTime);
      return accessor.getData(paths);
    } catch (DateTimeParseException e) {
      Log.info("Incorrectly formatted QueryDate Returning Empty DataSet");
      return Optional.empty();
    }
  }

  /**
   * @param swapIds the list of swapIds for a counter party
   * @param effectiveDate the effective date
   * @return a dataset of all the positions across all swap contracts that a specific counter party
   *     has for a specific effective date
   */
  public Optional<Dataset<Row>> getSwapContractsPositions(final Collection<Long> swapIds,
      final String effectiveDate) {

    ArrayList<String> paths = new ArrayList<>();
    for (long swapId : swapIds) {
      String path =
          baseFilePath + "positions/" + effectiveDate + "-" + swapId + "-" + "positions.jsonl";
      log.info("Adding item to positions path list: {}", path);
      paths.add(path);
    }
    Object[] arr = paths.toArray();
    return accessor.getData(Arrays.copyOf(arr, arr.length, String[].class));
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

  public Long getCounterPartyId(final String book, final Dataset<Row> counterParties) {
    Dataset<Row> counterParty = counterParties.select("counterparty_id")
        .where(counterParties.col("book").equalTo(book));
    if (counterParty.isEmpty()) {
      return -1L;
    } else {
      return counterParty.first().getAs("counterparty_id");
    }
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

  private Collection<Long> combineCounterPartySwapIds(final Optional<Dataset<Row>> swapContracts,
      final Long counterPartyId) {
    Dataset<Row> contracts = swapContracts.get();
    Dataset<Row> swapIdRows = contracts.select("swap_contract_id")
        .where(contracts.col("counterparty_id").equalTo(counterPartyId)).distinct();

    swapIdRows.cache();

    Dataset<Row> pivotSet =
        swapIdRows.withColumn("tempGroupCol", functions.lit(0)).withColumn("tempSumCol",
            functions.lit(0));

    List<String> stringList =
        Arrays.asList(pivotSet.groupBy("tempGroupCol").pivot("swap_contract_id")
            .sum("tempSumCol").drop("tempGroupCol").columns());
    return stringList.stream().map(s -> Long.valueOf(s)).collect(Collectors.toList());
  }

  private String[] getCashFlowFilePathsInRange(String queryDate, Collection<Long> swapIds)
      throws DateTimeParseException {

    FileStatus[] status = accessor.getStatusArray(baseFilePath + "/cashflows");
    ArrayList<String> fileNames = new ArrayList<>();
    YearMonth testDate = getQueryYearDate(queryDate);
    for (int i = 0; i < status.length; i++) {
      if (isValidCashflowsDateRange(testDate, swapIds, status[i].getPath().getName())) {
        fileNames.add(status[i].getPath().toString());
        log.info("Adding item to cashflows path list: {} ", status[i].getPath().toString());
      }
    }
    Object[] arr = fileNames.toArray();
    return Arrays.copyOf(arr, arr.length, String[].class);
  }


  private YearMonth getQueryYearDate(String queryDate) throws DateTimeParseException {
    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    return YearMonth.from(LocalDate.parse(queryDate, dateFormat));
  }

  private boolean isValidCashflowsDateRange(YearMonth testDate, Collection<Long> swapIds,
      String path) {
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



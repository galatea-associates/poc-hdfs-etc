package org.galatea.pochdfs.service.writer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.Set;
import org.galatea.pochdfs.utils.hdfs.IHdfsFilePathGetter;
import org.galatea.pochdfs.utils.hdfs.JsonMapper;
import org.mortbay.log.Log;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class LocalSwapFileWriter {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final SwapFilePathCreator pathCreator;
  private final JsonMapper objectMapper;

  private Map<String, ArrayDeque<String>> dataMap;
  private int BUFFER_SIZE = 1000;

  private long totalRecordsLogged = 0;
  private long systemStartTime;

  public void writeSwapData(final String localFolderPath, String targetBasePath) {
    systemStartTime = System.currentTimeMillis();
    File directory = new File(localFolderPath);
    File[] filesInDirectory = directory.listFiles();
    int filesProcessed = 0;
    for (File file : filesInDirectory) {
      if (file.isDirectory() == false) {
        writeSwapDataFromIndividualFile(file, targetBasePath);
        filesProcessed++;
        log.info("Total Files Processed: {} ", filesProcessed);
      }
    }
    log.info("******** Process Completed in {} ms ********",
        System.currentTimeMillis() - systemStartTime);
  }

  @SneakyThrows
  public void writeSwapDataFromIndividualFile(File file, final String targetBasePath) {
    String filename = file.getName();

    log.info("Reading data from {}", filename);

    if (filename.toLowerCase().contains("counterparties")) {
      writeStringToFile(new String(Files.readAllBytes(file.toPath())),
          pathCreator.createCounterpartyFilepath(),
          targetBasePath);
    } else if (filename.toLowerCase().contains("instruments")) {
      writeStringToFile(new String(Files.readAllBytes(file.toPath())),
          pathCreator.createInstrumentsFilepath(),
          targetBasePath);
    } else if (filename.toLowerCase().contains("contracts")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.constructSwapContractFilepath((int) jsonObject.get("counterparty_id"));
      });
    } else if (filename.toLowerCase().contains("positions")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.createPositionFilepath((String) jsonObject.get("effective_date"),
            (int) jsonObject.get("swap_contract_id"));
      });
    } else if (filename.toLowerCase().contains("cashflows")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.createCashFlowFilepath((String) jsonObject.get("effective_date"),
            (String) jsonObject.get("pay_date"), (int) jsonObject.get("swap_contract_id"));
      });

    } else {
      throw new FileNotFoundException("File " + filename + "not found");
    }
  }

  public void setBuffer(int size) {
    BUFFER_SIZE = size;
  }

  @SneakyThrows
  private void writeRecords(final File file, final String targetBasePath,
      final IHdfsFilePathGetter pathGetter) {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {

      long logTimeCounter = System.currentTimeMillis();
      dataMap = new HashMap<String, ArrayDeque<String>>();

      String jsonLine = reader.readLine();
      while (jsonLine != null) {

        Map<String, Object> jsonObject = objectMapper.getTimestampedObject(jsonLine);
        String filePath = pathGetter.getFilePath(jsonObject);
        String recordData = new StringBuilder(jsonLine).append("\n").toString();
        addDataToDataMap(targetBasePath + filePath, recordData);

        if (System.currentTimeMillis() - logTimeCounter > 1000) {
          givePastSecondUpdate();
          logTimeCounter = System.currentTimeMillis();
        }

        jsonLine = reader.readLine();
      }
      clearMap();
      log.info("******** File Processed in {} ms ********",
          System.currentTimeMillis() - systemStartTime);
      givePastSecondUpdate();
    }
  }

  private void givePastSecondUpdate() {
    long totalSeconds = (System.currentTimeMillis() - systemStartTime) / 1000;
    if (totalSeconds != 0) {
      log.info("******** Average Records logged per second {} ********",
          totalRecordsLogged / totalSeconds);
    } else {
      log.info("******** Average Records logged per second {} ********",
          totalRecordsLogged);
    }
    log.info("******** Totol Recrods Logged {} ******** ", totalRecordsLogged);
    log.info("******** Current Map Size {} ********", dataMap.size());
  }

  private void addDataToDataMap(String filePath, String data) {
    if (dataMap.containsKey(filePath)) {
      dataMap.get(filePath).add(data);
    } else {
      ArrayDeque<String> dataSet = new ArrayDeque<String>();
      dataSet.add(data);
      dataMap.put(filePath, dataSet);
    }
    checkMapSize(filePath);
  }

  private void clearMap() {
    log.info("******** Clearing map ********");
    for (String filePath : dataMap.keySet()) {
      writeDataSetToFile(filePath);
    }
    dataMap.clear();
  }

  private void checkMapSize(String filePath) {
    if (dataMap.get(filePath).size() >= BUFFER_SIZE) {
      writeDataSetToFile(filePath);
    }
//    if(dataMap.size() >= BUFFER_SIZE){
//      clearMap();
//    }
  }

  @SneakyThrows
  private void writeDataSetToFile(String filePath) {
    log.info("Writing Data to File {} ", filePath);
    Files.createDirectories(Paths.get(filePath).getParent());
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      for (String data : dataMap.get(filePath)) {
        if (filePath.toLowerCase().contains("cashflows")) {
          writer.write(data);
          totalRecordsLogged++;
        }
      }
      dataMap.put(filePath, new ArrayDeque<String>());
    }
  }

  @SneakyThrows
  private void writeStringToFile(final String data, final String filePath,
      final String targetBasePath) {
    log.info(targetBasePath);
    log.info(filePath);
    Files.createDirectories(Paths.get(targetBasePath + filePath).getParent());
    try (BufferedWriter writer = new BufferedWriter(
        new FileWriter(targetBasePath + filePath, true))) {
      writer.write(data);
    }
  }


}

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

  private Map<String, List<String>> dataMap;
  private int BUFFER_SIZE = 1000;

  private long totalRecordsLogged = 0;
  private long systemStartTime;

  @SneakyThrows
  public void writeSwapData(final String localFilePath, final String targetBasePath) {
    File file = new File(localFilePath);
    String filename = file.getName();

    log.info("Writing {} data", filename);

    if (filename.toLowerCase().contains("counterparties")) {
      writeStringToFile(new String(Files.readAllBytes(file.toPath())),
          pathCreator.createCounterpartyFilepath(),
          targetBasePath);
    } else if (filename.toLowerCase().contains("instruments")) {
      writeStringToFile(new String(Files.readAllBytes(file.toPath())),
          pathCreator.createInstrumentsFilepath(),
          targetBasePath);
    } else if (filename.toLowerCase().contains("swapcontracts")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.constructSwapContractFilepath((int) jsonObject.get("counterparty_id"));
      });
    } else if (filename.toLowerCase().contains("positions")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.createPositionFilepath((int) jsonObject.get("swap_contract_id"));
      });
    } else if (filename.toLowerCase().contains("cashflows")) {
      writeRecords(file, targetBasePath, (jsonObject) -> {
        return pathCreator.createCashFlowFilepath((int) jsonObject.get("swap_contract_id"));
      });
    } else {
      throw new FileNotFoundException("File " + filename + "not found");
    }
  }

  @SneakyThrows
  private void writeRecords(final File file, final String targetBasePath,
      final IHdfsFilePathGetter pathGetter) {

    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      long logTimeCounter = System.currentTimeMillis();

      dataMap = new HashMap<String, List<String>>();

      systemStartTime = System.currentTimeMillis();

      int recordCount = 0;
      String jsonLine = reader.readLine();

      while (jsonLine != null) {

        Long individualProcessStartTime = System.currentTimeMillis();

        Map<String, Object> jsonObject = objectMapper.getTimestampedObject(jsonLine);

        log.debug("Finished object mapping in {} ms",
            System.currentTimeMillis() - individualProcessStartTime);
        individualProcessStartTime = System.currentTimeMillis();

        String filePath = pathGetter.getFilePath(jsonObject);

        log.debug("Retrieved FilePath in {} ms",
            System.currentTimeMillis() - individualProcessStartTime);
        individualProcessStartTime = System.currentTimeMillis();
        StringBuilder builder = new StringBuilder(jsonLine);
        String recordData = builder.append("\n").toString();

        addDataToDataMap(targetBasePath + filePath, recordData);

        log.debug("Added object {} to Map", recordCount);
        log.debug("Wrote object to file in {} ms",
            System.currentTimeMillis() - individualProcessStartTime);
        log.debug("Total time to write one object file {} ms",
            System.currentTimeMillis() - systemStartTime);
        recordCount++;

        jsonLine = reader.readLine();

        if (System.currentTimeMillis() - logTimeCounter > 1000) {
          long totalSeconds = (System.currentTimeMillis() - systemStartTime)/1000;
          log.info("******** Average Records logged per second {} ********",
              totalRecordsLogged / totalSeconds);
          logTimeCounter = System.currentTimeMillis();
        }
      }
      clearMap();
      long totalSeconds = (System.currentTimeMillis() - systemStartTime)/1000;
      log.info("Process Completed in {} ms", System.currentTimeMillis() - systemStartTime);
      log.info("******** Average Records logged per second {} ********",
           totalRecordsLogged/ totalSeconds);
    }
  }

  private void addDataToDataMap(String filePath, String data) {
    if (dataMap.containsKey(filePath)) {
      dataMap.get(filePath).add(data);
    } else {
      List<String> dataSet = new LinkedList<String>();
      dataSet.add(data);
      dataMap.put(filePath, dataSet);
    }
    checkMapSize(filePath);
  }

  private void clearMap() {
    for (String filePath : dataMap.keySet()) {
      writeDataSetToFile(filePath);
    }
    dataMap.clear();
  }

  private void checkMapSize(String filePath) {
    if (dataMap.get(filePath).size() >= BUFFER_SIZE) {
      writeDataSetToFile(filePath);
    }
  }


  @SneakyThrows
  private void writeDataSetToFile(String filePath) {
    log.info("Writing Data to File {} ", filePath);
    Files.createDirectories(Paths.get(filePath).getParent());
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
      List<String> dataSet = dataMap.get(filePath);
      for (String data : dataSet) {
        writer.write(data);
        totalRecordsLogged++;
      }
      dataMap.put(filePath, new LinkedList<String>());
      //dataMap.remove(filePath);
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

  public void setBuffer(int size){
    BUFFER_SIZE = size;
  }




}

package org.galatea.pochdfs.speedTest;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter

public class SpeedTest {

  private long numNodes = 0;
  private long numCores = 0;
  private int partitions;
  private int parallelism = 0;
  private String date = "N/A";
  private long totalRunTime = 0;
  private long totalBookRead = 0;
  private long positionsRead = 0;
  private long cashFlowRead = 0;
  private long instrumentRead = 0;
  private long swapContractRead = 0;
  private long counterpartyRead = 0;
  private long listingLeafTime = 0;
  private long pivotTime = 0;
  private long enrichedPositionTime = 0;
  private long cashCreationTime = 0;
  private long joinTime = 0;


  public Object[] getAllValues() {
    return new Object[] {numNodes, numCores,partitions,parallelism, date, totalRunTime, totalBookRead,
        positionsRead, cashFlowRead, instrumentRead, swapContractRead, cashCreationTime,
        enrichedPositionTime, joinTime,listingLeafTime};

  }
}

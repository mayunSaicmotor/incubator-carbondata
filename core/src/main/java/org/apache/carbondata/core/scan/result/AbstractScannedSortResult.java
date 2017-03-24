/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.core.scan.result;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public abstract class AbstractScannedSortResult extends AbstractScannedResult {

  public AbstractScannedSortResult(BlockExecutionInfo blockExecutionInfo) {
    super(blockExecutionInfo);
    
    this.allSortDimensionBlocksIndexes = blockExecutionInfo.getAllSortDimensionBlocksIndexes();
  }
  
  
  
  // TODO
  private SortOrderType sortType = SortOrderType.NONE;
  private BlocksChunkHolder blocksChunkHolder;

  //private boolean loadDataDelay = false;

  private boolean filterQueryFlg = false;

  private boolean sortByDictionaryDimensionFlg = false;
  private boolean sortByNoDictionaryDimensionFlg = false;

  public boolean isSortByNoDictionaryDimensionFlg() {
    return sortByNoDictionaryDimensionFlg;
  }

  public void setSortByNoDictionaryDimensionFlg(boolean sortByNoDictionaryDimensionFlg) {
    this.sortByNoDictionaryDimensionFlg = sortByNoDictionaryDimensionFlg;
  }

  public boolean isSortByDictionaryDimensionFlg() {
    return sortByDictionaryDimensionFlg;
  }

  public void setSortByDictionaryDimensionFlg(boolean sortByDictionaryDimensionFlg) {
    this.sortByDictionaryDimensionFlg = sortByDictionaryDimensionFlg;
  }

  public boolean isFilterQueryFlg() {
    return filterQueryFlg;
  }

  public void setFilterQueryFlg(boolean filterQueryFlg) {
    this.filterQueryFlg = filterQueryFlg;
  }

  // from 0 to n-1
  private int maxLogicalRowIdByLimit = -1;

  public int getMaxLogicalRowIdByLimit() {
    if (maxLogicalRowIdByLimit >= 0) {
      return maxLogicalRowIdByLimit;
    }
    return numberOfRows[pageCounter];
  }

  public void setMaxLogicalRowIdByLimit(int maxLogicalRowIdByLimit) {
    this.maxLogicalRowIdByLimit = maxLogicalRowIdByLimit;
  }

  public boolean isLoadDataDelay() {
    return this.blocksChunkHolder.isLoadDataDelay();
  }

  public void setLoadDataDelay(boolean loadDataDelay) {
    this.blocksChunkHolder.setLoadDataDelay(loadDataDelay);
  }

  public void resetLoadDataDelay() {
    this.blocksChunkHolder.setLoadDataDelay(false);
  }

  public BlocksChunkHolder getBlocksChunkHolder() {
    return blocksChunkHolder;
  }

  public void setBlocksChunkHolder(BlocksChunkHolder blocksChunkHolder) {
    this.blocksChunkHolder = blocksChunkHolder;
  }

  public SortOrderType getSortType() {
    return sortType;
  }

  public void setSortType(SortOrderType sortType) {
    this.sortType = sortType;
  }

  private Long nodeNumber;
  private String blockletNodeId;

  public String getBlockletNodeId() {
    return blockletNodeId;
  }

  public void setBlockletNodeId(String blockletNodeId) {
    this.blockletNodeId = blockletNodeId;
  }

  public Long getNodeNumber() {
    return nodeNumber;
  }

  public void setNodeNumber(Long nodeNumber) {
    this.nodeNumber = nodeNumber;
  }

  public int getCurrentRow() {
    return currentRow;
  }

  public void setCurrentRow(int currentRow) {
    this.currentRow = currentRow;
  }

  protected boolean descSortFlg = false;

  protected int currentLogicRowId = -1;

  public int getCurrentLogicRowId() {
    return currentLogicRowId;
  }

  public void setCurrentLogicRowId(int currentLogicRowId) {
    this.currentLogicRowId = currentLogicRowId;
  }

/*  public int[][] getRowMapping() {
    return rowMapping;
  }

  public void setRowMapping(int[][] rowMapping) {
    this.rowMapping = rowMapping;
  }*/

  protected String stopKey = null;

  public String getStopKey() {
    return stopKey;
  }

  public void setStopKey(String stopKey) {
    if (stopKey == null) {
      stopKey = "";
    }
    this.stopKey = stopKey;
  }

  int sortDimentionIndex;

  protected String currentSortDimentionKey = null;

  protected boolean currentSortDimentionKeyChgFlg = false;

  public boolean isCurrentSortDimentionKeyChgFlg() {
    return currentSortDimentionKeyChgFlg;
  }

  public void setCurrentSortDimentionKeyChgFlg(boolean currentSortDimentionKeyChgFlg) {
    this.currentSortDimentionKeyChgFlg = currentSortDimentionKeyChgFlg;
  }

  public String getCurrentSortDimentionKey() {
    return currentSortDimentionKey;
  }

  public void setCurrentSortDimentionKey(String currentSortDimentionKey) {
    this.currentSortDimentionKey = currentSortDimentionKey;
  }

  protected int sortSingleDimensionBlocksIndex = -1;
  protected DimensionColumnDataChunk sortDimention = null;
  protected boolean pauseProcessForSortFlg = false;
  protected int[] pausedCompleteKey = null;

  protected String[] pausedNoDictionaryKeys = null;

  protected ScannedResultCollector scannerResultAggregator;

  public boolean isPauseProcessForSortFlg() {
    return pauseProcessForSortFlg;
  }

  public void resetPauseProcessForSortFlg() {
    pauseProcessForSortFlg = false;
  }

  public void setPauseProcessForSortFlg(boolean pauseProcessForSortFlg) {
    this.pauseProcessForSortFlg = pauseProcessForSortFlg;
  }

  public void resetPauseData() {
    this.pausedCompleteKey = null;
    this.pausedNoDictionaryKeys = null;
    this.pauseProcessForSortFlg = false;
    // rowCounter++;
  }

  public String[] getPausedNoDictionaryKeys() {
    return pausedNoDictionaryKeys;
  }

  public void setPausedNoDictionaryKeys(String[] pausedNoDictionaryKeys) {
    this.pausedNoDictionaryKeys = pausedNoDictionaryKeys;
  }

  public int[] getPausedCompleteKey() {

    return pausedCompleteKey;
  }

  public void setPausedCompleteKey(int[] pausedCompleteKey) {
    this.pausedCompleteKey = pausedCompleteKey;
  }

  public ScannedResultCollector getScannerResultAggregator() {
    return scannerResultAggregator;
  }

  public boolean hasNextForSort() {

    return rowCounter < numberOfRowsInCurrentPage;
  }

  
  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * in integer array format which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected int[] getDictionaryKeyIntegerArray(int rowId) {
      
  
    int[] completeKey = new int[totalDimensionsSize];
    int column = 0;
    
    caculateCurrentRowId(rowId);
    //TODO
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {

      column = dataChunks[dictionaryColumnBlockIndexes[i]][pageCounter].fillConvertedChunkData(
          currentLogicRowId, column, completeKey,
          columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));

    }
    
    // for dictionary dimension all key is number, but for no dictionary dimension, it is string
    if(sortByDictionaryDimensionFlg){
        
        if (sortSingleDimensionBlocksIndex >= 0
                && ((!this.descSortFlg && completeKey[sortDimentionIndex] > Integer.parseInt(stopKey))
                        || (this.descSortFlg && completeKey[sortDimentionIndex] < Integer.parseInt(stopKey)))) {
            pauseProcessCollectData(completeKey);
            //return completeKey;
        }
    }

    rowCounter++;
    //System.out.println("completeKey: "+completeKey.toString());
    return completeKey;
  }
 
public void caculateCurrentRowId(int rowId) {
    
    currentPhysicalRowId = rowId;
    //for sort query push down sort
    if(sortSingleDimensionBlocksIndex >= 0){
        
        //baseSortDimentionInvertedIndexes = dataChunks[sortSingleDimensionBlocksIndex].getAttributes().getInvertedIndexes();
        
        if(baseSortDimentionDataChunk.isExplicitSorted()){
            
      
    
            // for filter query
            // if(this.rowMapping != null && this.rowMapping[pageCounter] != null&& this.rowMapping[pageCounter].length > 0){
            if(this.filterQueryFlg){    
                //caculateCurrentRowIdForFilterQuery();
              // set logical row id default value
              this.currentLogicRowId=baseSortDimentionDataChunk.getInvertedIndex(this.physicalRowMapping[pageCounter][currentPhysicalRowId]);  
            // for no filter query and baseSortDimentionInvertedIndexes != null
            }else{
                
                if(this.descSortFlg){               

                    currentPhysicalRowId = numberOfRows[pageCounter]- rowId -1;    
                }
                //this.currentLogicRowId=baseSortDimentionDataChunk.getInvertedIndex(currentPhysicalRowIdForSortDimension);
            }
            
        //when baseSortDimentionInvertedIndexes = null  
        }else{
            
            // for filter query
            //if(this.rowMapping != null && this.rowMapping[pageCounter] != null&& this.rowMapping[pageCounter].length > 0){
            if(this.filterQueryFlg){
                caculateCurrentRowIdForNoInvertedIndexFilterQuery();
            }else{
                if(this.descSortFlg){
                    
                    currentPhysicalRowId = numberOfRows[pageCounter]- rowId -1;
                }           
                this.currentLogicRowId=currentPhysicalRowId;    
            }

        }
        
    //for no sort query
    }else{
        //TODO
        
        // for filter query
        //if(this.rowMapping != null && this.rowMapping[pageCounter] != null&& this.rowMapping[pageCounter].length > 0){
      if(this.filterQueryFlg){
            this.currentLogicRowId= rowMapping[pageCounter][rowId];
        }else{
            
            this.currentLogicRowId=currentPhysicalRowId;
        }
    }
    //return rowId;
}

/*private void caculateCurrentRowIdForFilterQuery() {
  if(this.descSortFlg){
    int findIndex;
      for(int physicalRowId = currentPhysicalIndexForFilter; physicalRowId >=0; physicalRowId--){
          // this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
        findIndex = Arrays.binarySearch(this.rowMapping[pageCounter], baseSortDimentionDataChunk.getInvertedIndex(physicalRowId));
          if(findIndex >= 0){
              //System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
              this.currentPhysicalIndexForFilter = physicalRowId-1;
              this.currentPhysicalRowId = physicalRowId;
              this.currentLogicRowId=this.rowMapping[pageCounter][findIndex];  
              break;
          }
          
      }
  } else {
      // TODO
    int findIndex;
      for(int index = currentPhysicalIndexForFilter; index < numberOfRows[pageCounter]; index++){
      //for(int index = currentPhysicalIndexForFilter; index < baseSortDimentionInvertedIndexes.length; index++){
          // this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
        findIndex =Arrays.binarySearch(this.rowMapping[pageCounter], baseSortDimentionDataChunk.getInvertedIndex(index));
          if(findIndex >= 0){
              //System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
              this.currentPhysicalIndexForFilter = index+1;
              this.currentPhysicalRowId = index;
              this.currentLogicRowId=this.rowMapping[pageCounter][findIndex];  
              break;
          }
          
      }
  }
}*/



private void caculateCurrentRowIdForNoInvertedIndexFilterQuery() {
  if(this.descSortFlg){
      if(this.currentPhysicalIndexForFilter >= rowMapping[pageCounter].length){
          this.currentPhysicalIndexForFilter = this.rowMapping[pageCounter].length -1;
      }
      this.currentPhysicalRowId = this.rowMapping[pageCounter][currentPhysicalIndexForFilter];
      this.currentLogicRowId=this.currentPhysicalRowId;
      this.currentPhysicalIndexForFilter --;
  } else {
      this.currentPhysicalRowId = this.rowMapping[pageCounter][currentPhysicalIndexForFilter];
      this.currentLogicRowId=this.currentPhysicalRowId;
      this.currentPhysicalIndexForFilter ++;
  }
}

public void pauseProcessCollectData(int[] completeKey) {
  currentSortDimentionKey = Integer.toString(completeKey[sortDimentionIndex]);
  currentSortDimentionKeyChgFlg = true;
  pauseProcessForSortFlg = true;
  pausedCompleteKey = completeKey;
  //rowCounter++;
}

public void pauseProcessCollectData(String[] noDictonaryKeys) {
  currentSortDimentionKey = noDictonaryKeys[sortDimentionIndex];
  currentSortDimentionKeyChgFlg = true;
  pauseProcessForSortFlg = true;
  pausedNoDictionaryKeys = noDictonaryKeys;
  //rowCounter++;
}
  /**
   * @return dictionary key array for all the dictionary dimension in integer
   *         array forat selected in query
   */
  // TODO
  public int[] getDictionaryKeyIntegerArrayHasLimitKey(String stopKey) {
    if (stopKey == null) {
      if (descSortFlg) {
        if (this.sortByDictionaryDimensionFlg) {
          this.stopKey = String.valueOf(Integer.MIN_VALUE);
        } else {
          this.stopKey = CarbonCommonConstants.MIN_STR;
        }

      } else {

        if (this.sortByDictionaryDimensionFlg) {
          this.stopKey = String.valueOf(Integer.MAX_VALUE);
        } else {
          this.stopKey = CarbonCommonConstants.MAX_STR;
        }

      }

    } else {

      this.stopKey = stopKey;
    }
    ++currentRow;
    // incrementCurrentRowBySortType();
    return getDictionaryKeyIntegerArray(currentRow);
  }


  public List<Object[]> collectSortedData(int batchSize, String stopKey) throws IOException {
    this.getBlocksChunkHolder().getBlockletScanner().readBlockletForLazyLoad(this);
    return this.scannerResultAggregator.collectSortData(this, batchSize, stopKey);
  }

  public void setScannerResultAggregator(ScannedResultCollector scannerResultAggregator) {
    this.scannerResultAggregator = scannerResultAggregator;
  }

  protected DimensionColumnDataChunk baseSortDimentionDataChunk = null;
  protected int currentPhysicalIndexForFilter = 0;

  // current physical row id for sort dimension
  protected int currentPhysicalRowId = 0;

  /**
   * sorted dimension indexes
   */
  private int[] allSortDimensionBlocksIndexes;

  public int[] getAllSortDimensionBlocksIndexes() {
    return allSortDimensionBlocksIndexes;
  }

  public void setAllSortDimensionBlocksIndexes() {
    // this.allSortDimensionBlocksIndexes = allSortDimensionBlocksIndexes;

    // only consider one sort dimension currently
    if (allSortDimensionBlocksIndexes != null && allSortDimensionBlocksIndexes.length > 0) {
      sortSingleDimensionBlocksIndex = allSortDimensionBlocksIndexes[0];
      sortDimention = dataChunks[sortSingleDimensionBlocksIndex][pageCounter];
      baseSortDimentionDataChunk = dataChunks[sortSingleDimensionBlocksIndex][pageCounter];
      for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
        if (dictionaryColumnBlockIndexes[i] == sortSingleDimensionBlocksIndex) {
          sortDimentionIndex = i;
          sortByDictionaryDimensionFlg = true;
          break;
        }
      }
      if (!sortByDictionaryDimensionFlg) {

        for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
          if (noDictionaryColumnBlockIndexes[i] == sortSingleDimensionBlocksIndex) {
            sortByNoDictionaryDimensionFlg = true;
            break;
          }
        }
      }

    }
  }

  /*
   * public void setCurrentDictionaryKeyForSortDimention() {
   * 
   * 
   * int[] keyArr = new int[1];
   * sortDimention.fillConvertedChunkData(currentRow+1, 0, keyArr,
   * columnGroupKeyStructureInfo.get(sortSingleDimensionBlocksIndex));
   * 
   * this.currentSortDimentionKey = keyArr[0]; }
   */

  public void initCurrentKeyForSortDimention(SortOrderType sortType) {

    this.sortType = sortType;
    if(this.filterQueryFlg){
      numberOfRowsInCurrentPage = this.physicalRowMapping[pageCounter].length;
    }else{
      numberOfRowsInCurrentPage = this.numberOfRows[pageCounter];
    }
    if (SortOrderType.DSC.equals(sortType)) {
      this.descSortFlg = true;
      currentPhysicalIndexForFilter = numberOfRows[pageCounter] - 1;


    }
    if (this.sortByDictionaryDimensionFlg) {
          
          this.currentSortDimentionKey = Integer.toString(sortDimention.getSurrogateByPhysicalId(getStartPhysicalRowId()));
          // System.out.println("currentSortDimentionKey: " + currentSortDimentionKey);
        } else if (this.sortByNoDictionaryDimensionFlg) {
    
          this.currentSortDimentionKey = new String(sortDimention.getChunkDataByPhysicalRowId(getStartPhysicalRowId()));
    
        } else {
    
          // TODO
          this.currentSortDimentionKey = new String(sortDimention.getChunkDataByPhysicalRowId(getStartPhysicalRowId()));
    
        }
    // System.out.println("currentSortDimentionKey: " + currentSortDimentionKey);
  }


private int getStartPhysicalRowId() {
    int startIndex;
    if(descSortFlg){
      if(this.filterQueryFlg){
        startIndex =this.physicalRowMapping[pageCounter][this.physicalRowMapping[pageCounter].length-1];
      }else{
        startIndex  = sortDimention.getTotalRowNumber() - 1;
      }
    }else{
      if(this.filterQueryFlg){
        startIndex =this.physicalRowMapping[pageCounter][0];
      }else{
        startIndex  = rowCounter;
      }
      
    }
    
    return startIndex;
/*    if (sortDimention.isExplicitSorted()) {
      return sortDimention
          .getInvertedIndex(descSortFlg ? sortDimention.getTotalRowNumber() - 1 : rowCounter);
    }
    return descSortFlg ? sortDimention.getTotalRowNumber() - 1 : rowCounter;*/
  }
  
  public void decrementRowCounter() {
    rowCounter--;
  }

  public void incrementRowCounter() {
    rowCounter++;
  }


  protected int numberOfRowsInCurrentPage = 0;
  protected int validPageCnt = 0;
  public void setValidPageCnt(int validPageCnt) {
    this.validPageCnt = validPageCnt;
  }
  
  public int getValidPageCnt() {
    return  validPageCnt;
  }
  
}

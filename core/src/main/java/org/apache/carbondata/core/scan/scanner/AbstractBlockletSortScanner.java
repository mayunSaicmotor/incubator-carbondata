/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.core.scan.scanner;

import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.mutate.data.BlockletDeleteDeltaCacheLoader;
import org.apache.carbondata.core.mutate.data.DeleteDeltaCacheLoaderIntf;
import org.apache.carbondata.core.scan.collector.impl.DictionaryBasedSortResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedSortResult;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;

/**
 * Blocklet scanner class to process the block
 */
public abstract class AbstractBlockletSortScanner extends AbstractBlockletScanner {

 
  public AbstractBlockletSortScanner(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
    // TODO Auto-generated constructor stub
  }

  @Override public AbstractScannedSortResult[] scanBlockletForSort(BlocksChunkHolder blocksChunkHolder, SortOrderType orderType)
      throws IOException, FilterUnsupportedException {
    //AbstractScannedSortResult scannedSortDimResult = new NonFilterQueryScannedSortResult(blockExecutionInfo);
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(totalBlockletStatistic);
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(validScannedBlockletStatistic);
//    scannedSortDimResult.setBlockletId(
//        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
//            .getDataBlock().nodeNumber());
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        blocksChunkHolder.getDimensionRawDataChunk();
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
      }
    }

    int[][] measureIndexRange = blocksChunkHolder.getBlockExecutionInfo().getAllSelectedMeasureBlocksIndexes();
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureIndexRange[measureIndexRange.length-1][1] + 1][];
    
    
    AbstractScannedSortResult[] scannedResults = new AbstractScannedSortResult[dimensionRawColumnChunks[blocksChunkHolder
        .getAllSortDimensionBlocksIndexes()[0]].getPagesCount()];
    for(int i =0; i<scannedResults.length;i++){
      scannedResults[i] = new NonFilterQueryScannedSortResult(blockExecutionInfo);
    }
    
    transferToScanResultArr(blocksChunkHolder, orderType, dimensionRawColumnChunks,
        dimensionColumnDataChunks, measureColumnDataChunks, scannedResults);
    

/*    int[] numberOfRows = new int[] { blocksChunkHolder.getDataBlock().nodeSize() };
    if (blockExecutionInfo.getAllSelectedDimensionBlocksIndexes().length > 0) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          numberOfRows = dimensionRawColumnChunks[i].getRowCount();
          break;
        }
      }
    }
    scannedSortDimResult.setNumberOfRows(numberOfRows);*/

/*
    if(!scannedSortDimResult.isFilterQueryFlg()){
      int[] invertedIndexesReverse = dimensionColumnDataChunks[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]].getAttributes().getInvertedIndexesReverse();
      if(invertedIndexesReverse!=null){
        scannedSortDimResult.setMaxLogicalRowIdByLimit(invertedIndexesReverse.length-1);
      }else{
          
        scannedSortDimResult.setMaxLogicalRowIdByLimit(scannedSortDimResult.getDimensionChunks()[scannedSortDimResult.getAllSortDimensionBlocksIndexes()[0]].getTotalRowNumber());
      }
      
  }*/

    return scannedResults;
  }

  public void transferToScanResultArr(BlocksChunkHolder blocksChunkHolder,
      SortOrderType orderType, DimensionRawColumnChunk[] dimensionRawColumnChunks,
      DimensionColumnDataChunk[][] dimensionColumnDataChunks,
      MeasureColumnDataChunk[][] measureColumnDataChunks, AbstractScannedSortResult[] scannedResults) {

    AbstractScannedSortResult scannedSortDimResult;
    String blockletPrefix = blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR;
    DictionaryBasedSortResultCollector dict = new DictionaryBasedSortResultCollector(blockExecutionInfo);
    

    int[][] rowMapping = scannedResults[0].getIndexes();
    boolean filterFlg = (blockExecutionInfo.getFilterExecuterTree() != null);
    int size = filterFlg ? rowMapping.length :scannedResults.length;
    int scannedResultIndex = 0;
    for (int i = 0; i < size; i++) {

      if (filterFlg) {
        if (rowMapping[i] == null || rowMapping[i].length == 0) {
          continue;
        } else {
          scannedResults[scannedResultIndex].setFilterQueryFlg(true);
          scannedResults[scannedResultIndex].setPhysicalRowMapping(scannedResults[0].getPhysicalRowMapping());
          scannedResults[scannedResultIndex].setIndexes(rowMapping);
          scannedResults[scannedResultIndex].setNumberOfRows(scannedResults[0].getNumberOfRows(), scannedResults[0].numberOfOutputRows());
        }
      }

      scannedSortDimResult = scannedResults[scannedResultIndex];
      scannedResultIndex ++ ;
      if(scannedSortDimResult == null){
        continue;
      }
      scannedSortDimResult.setBlockletId(
          blockletPrefix + blocksChunkHolder
              .getDataBlock().nodeNumber() + CarbonCommonConstants.FILE_SEPARATOR + i);
      scannedSortDimResult.setDimensionChunks(dimensionColumnDataChunks);
      scannedSortDimResult.setNumberOfRows(dimensionRawColumnChunks[blocksChunkHolder
          .getAllSortDimensionBlocksIndexes()[0]].getRowCount());
      scannedSortDimResult.setRawColumnChunks(dimensionRawColumnChunks);
      scannedSortDimResult.setScannerResultAggregator(dict);
      scannedSortDimResult.setPageCounter(i);
      //TODO
      scannedSortDimResult.setBlocksChunkHolder(blocksChunkHolder);
      scannedSortDimResult.setLoadDataDelay(true);
      //scannedSortDimResult.setBlockletNodeId(blocksChunkHolder.getBlockletNodeId());
      scannedSortDimResult.setAllSortDimensionBlocksIndexes();
      scannedSortDimResult.initCurrentKeyForSortDimention(orderType);
      
      scannedSortDimResult.setMeasureChunks(measureColumnDataChunks);
      //scannedResults[scannedResultIndex] = scannedSortDimResult;
    }
  }

  
 
  
  @Override public void readBlockletForSort(BlocksChunkHolder blocksChunkHolder) throws IOException {
    
    int sortDimIndex = blocksChunkHolder.getAllSortDimensionBlocksIndexes()[0];
    if (null == blocksChunkHolder.getDimensionRawDataChunk()[sortDimIndex]) {
      blocksChunkHolder.getDimensionRawDataChunk()[sortDimIndex] = blocksChunkHolder.getDataBlock()
          .getDimensionChunk(blocksChunkHolder.getFileReader(), sortDimIndex);
    }
    /*
    int[][] allSortDimensionBlocksIndexes = new int[1][2];
    allSortDimensionBlocksIndexes[0][0] = blocksChunkHolder.getAllSortDimensionBlocksIndexes()[0];
    allSortDimensionBlocksIndexes[0][1] = blocksChunkHolder.getAllSortDimensionBlocksIndexes()[0];
    DimensionRawColumnChunk[] dimensionRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            allSortDimensionBlocksIndexes);
    blocksChunkHolder.setDimensionRawDataChunk(dimensionRawColumnChunks);*/
  }
  
  
/*  @Override public void readBlockletForLazyLoad(AbstractScannedSortResult scannedResult) throws IOException {
    
    BlocksChunkHolder blocksChunkHolder = scannedResult.getBlocksChunkHolder();
    
    DimensionRawColumnChunk[] dimensionRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes());


    MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getMeasureChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedMeasureBlocksIndexes());
    
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureRawColumnChunks.length][];
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      if (measureRawColumnChunks[i] != null) {
        measureColumnDataChunks[i] = measureRawColumnChunks[i].convertToMeasureColDataChunks();
      }
    }
    scannedResult.setMeasureChunks(measureColumnDataChunks);
  }*/

  // TODO lazy load other columns data except sort dimension
  @Override  public synchronized void readBlockletForLazyLoad(AbstractScannedSortResult scannedResult)
      throws IOException {
    if (scannedResult.isLoadDataDelay()) {
      //indexesGroup ＝ scannedResult.getRowMapping()
      BlocksChunkHolder blocksChunkHolder = scannedResult.getBlocksChunkHolder();
      BlockExecutionInfo blockExecutionInfo = blocksChunkHolder.getBlockExecutionInfo();
      DimensionColumnDataChunk[][] dimensionColumnDataChunks = scannedResult.getDimensionChunks();
      MeasureColumnDataChunk[][] measureColumnDataChunks = scannedResult.getMeasureChunks();
      
      // loading delete data cache in blockexecutioninfo instance
      DeleteDeltaCacheLoaderIntf deleteCacheLoader =
          new BlockletDeleteDeltaCacheLoader(scannedResult.getBlockletId(),
              blocksChunkHolder.getDataBlock(), blockExecutionInfo.getAbsoluteTableIdentifier());
      deleteCacheLoader.loadDeleteDeltaFileDataToCache();
      scannedResult
          .setBlockletDeleteDeltaCache(blocksChunkHolder.getDataBlock().getDeleteDeltaDataCache());

      FileHolder fileReader = blocksChunkHolder.getFileReader();
      int[][] allSelectedDimensionBlocksIndexes =
          blockExecutionInfo.getAllSelectedDimensionBlocksIndexes();
      DimensionRawColumnChunk[] projectionListDimensionChunk = blocksChunkHolder.getDataBlock()
          .getDimensionChunks(fileReader, allSelectedDimensionBlocksIndexes);

      DimensionRawColumnChunk[] dimensionRawColumnChunks =
          new DimensionRawColumnChunk[blockExecutionInfo.getTotalNumberDimensionBlock()];
      // read dimension chunk blocks from file which is not present
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (null != blocksChunkHolder.getDimensionRawDataChunk()[i]) {
          dimensionRawColumnChunks[i] = blocksChunkHolder.getDimensionRawDataChunk()[i];
        }
      }
      for (int i = 0; i < allSelectedDimensionBlocksIndexes.length; i++) {
        for (int j = allSelectedDimensionBlocksIndexes[i][0];
             j <= allSelectedDimensionBlocksIndexes[i][1]; j++) {
          dimensionRawColumnChunks[j] = projectionListDimensionChunk[j];
        }
      }
      /**
       * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
       * then loading them
       */
      int[] projectionListDimensionIndexes = blockExecutionInfo.getProjectionListDimensionIndexes();
      int projectionListDimensionIndexesLength = projectionListDimensionIndexes.length;
      for (int i = 0; i < projectionListDimensionIndexesLength; i++) {
        if (null == dimensionRawColumnChunks[projectionListDimensionIndexes[i]]) {
          dimensionRawColumnChunks[projectionListDimensionIndexes[i]] =
              blocksChunkHolder.getDataBlock()
                  .getDimensionChunk(fileReader, projectionListDimensionIndexes[i]);
        }
      }
      MeasureRawColumnChunk[] measureRawColumnChunks =
          new MeasureRawColumnChunk[blockExecutionInfo.getTotalNumberOfMeasureBlock()];
      int[][] allSelectedMeasureBlocksIndexes =
          blockExecutionInfo.getAllSelectedMeasureBlocksIndexes();
      MeasureRawColumnChunk[] projectionListMeasureChunk = blocksChunkHolder.getDataBlock()
          .getMeasureChunks(fileReader, allSelectedMeasureBlocksIndexes);
      // read the measure chunk blocks which is not present
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (null != blocksChunkHolder.getMeasureRawDataChunk()[i]) {
          measureRawColumnChunks[i] = blocksChunkHolder.getMeasureRawDataChunk()[i];
        }
      }
      for (int i = 0; i < allSelectedMeasureBlocksIndexes.length; i++) {
        for (int j = allSelectedMeasureBlocksIndexes[i][0];
             j <= allSelectedMeasureBlocksIndexes[i][1]; j++) {
          measureRawColumnChunks[j] = projectionListMeasureChunk[j];
        }
      }
      /**
       * in case projection if the projected measure are not loaded in the measureColumnDataChunk
       * then loading them
       */
      int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
      int projectionListMeasureIndexesLength = projectionListMeasureIndexes.length;
      for (int i = 0; i < projectionListMeasureIndexesLength; i++) {
        if (null == measureRawColumnChunks[projectionListMeasureIndexes[i]]) {
          measureRawColumnChunks[projectionListMeasureIndexes[i]] = blocksChunkHolder.getDataBlock()
              .getMeasureChunk(fileReader, projectionListMeasureIndexes[i]);
        }
      }
      
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
        }
      }

      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (measureRawColumnChunks[i] != null) {
          measureColumnDataChunks[i] = measureRawColumnChunks[i].convertToMeasureColDataChunks();
        }
      }
 /*     BlocksChunkHolder blocksChunkHolder = scannedResult.getBlocksChunkHolder();
      BlockExecutionInfo blockExecutionInfo = blocksChunkHolder.getBlockExecutionInfo();
      
      DimensionRawColumnChunk[] dimensionRawColumnChunks = blocksChunkHolder.getDataBlock()
          .getDimensionChunks(blocksChunkHolder.getFileReader(),
              blockExecutionInfo.getAllSelectedDimensionBlocksIndexes());
      
      
      DimensionColumnDataChunk[][] dimensionColumnDataChunks = scannedResult.getDimensionChunks();
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
        }
      }

      MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getDataBlock()
          .getMeasureChunks(blocksChunkHolder.getFileReader(),
              blockExecutionInfo.getAllSelectedMeasureBlocksIndexes());
      
      MeasureColumnDataChunk[][] measureColumnDataChunks = scannedResult.getMeasureChunks();
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (measureRawColumnChunks[i] != null) {
          measureColumnDataChunks[i] = measureRawColumnChunks[i].convertToMeasureColDataChunks();
        }
      }
      scannedResult.setMeasureChunks(measureColumnDataChunks);
      blocksChunkHolder.setMeasureRawDataChunk(measureRawColumnChunks);
      // loading delete data cache in blockexecutioninfo instance
      DeleteDeltaCacheLoaderIntf deleteCacheLoader = new BlockletDeleteDeltaCacheLoader(
          scannedResult.getBlockletId(), blocksChunkHolder.getDataBlock(),
          blockExecutionInfo.getAbsoluteTableIdentifier());
      deleteCacheLoader.loadDeleteDeltaFileDataToCache();
      scannedResult
          .setBlockletDeleteDeltaCache(blocksChunkHolder.getDataBlock().getDeleteDeltaDataCache());*/
      scannedResult.resetLoadDataDelay();
    }
//    else{
//      
//      scannedResult.setMeasureChunks(measureColumnDataChunks);
//    }
  }

  
  
//  /**
//   * scanner result
//   */
//  protected AbstractScannedSortResult scannedResult;
//
//  public AbstractScannedSortResult  getScannedResultAfterProcessFilter(){
//      
//      return scannedResult;
//      
//  }

}

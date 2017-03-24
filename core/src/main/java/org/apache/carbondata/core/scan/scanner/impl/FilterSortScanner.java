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

package org.apache.carbondata.core.scan.scanner.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.result.impl.FilterQueryScannedSortResult;
import org.apache.carbondata.core.scan.scanner.AbstractBlockletSortScanner;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used for filter query processing
 * this class will be first apply the filter then it will read the block if
 * required and return the scanned result
 */
public class FilterSortScanner extends AbstractBlockletSortScanner {

  /**
   * filter tree
   */
  private FilterExecuter filterExecuter;
  /**
   * this will be used to apply min max
   * this will be useful for dimension column which is on the right side
   * as node finder will always give tentative blocks, if column data stored individually
   * and data is in sorted order then we can check whether filter is in the range of min max or not
   * if it present then only we can apply filter on complete data.
   * this will be very useful in case of sparse data when rows are
   * repeating.
   */
  private boolean isMinMaxEnabled;


  public FilterSortScanner(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfo);
    // to check whether min max is enabled or not
    String minMaxEnableValue = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED,
            CarbonCommonConstants.MIN_MAX_DEFAULT_VALUE);
    if (null != minMaxEnableValue) {
      isMinMaxEnabled = Boolean.parseBoolean(minMaxEnableValue);
    }
    // get the filter tree
    this.filterExecuter = blockExecutionInfo.getFilterExecuterTree();
    this.queryStatisticsModel = queryStatisticsModel;
  }

/*  *//**
   * Below method will be used to process the block
   *
   * @param blocksChunkHolder block chunk holder which holds the data
   * @throws FilterUnsupportedException
   *//*
  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, FilterUnsupportedException {
    return fillScannedResult(blocksChunkHolder);
  }*/
/*
  @Override
  public AbstractScannedSortResult[] scanBlockletForSort(BlocksChunkHolder blocksChunkHolder,
      SortOrderType orderType) throws IOException, FilterUnsupportedException {
    applyFilter(blocksChunkHolder);
    return new FilterQueryScannedSortResult[0];
  }*/
  @Override public boolean isScanRequired(BlocksChunkHolder blocksChunkHolder) throws IOException {
    // apply min max
    if (isMinMaxEnabled) {
      BitSet bitSet = this.filterExecuter
          .isScanRequired(blocksChunkHolder.getDataBlock().getColumnsMaxValue(),
              blocksChunkHolder.getDataBlock().getColumnsMinValue());
      if (bitSet.isEmpty()) {
        CarbonUtil.freeMemory(blocksChunkHolder.getDimensionRawDataChunk(),
            blocksChunkHolder.getMeasureRawDataChunk());
        return false;
      }
    }
    
    // read filtered data
    this.filterExecuter.readBlocks(blocksChunkHolder);
    // is ScanRequired then read the data
    readBlockletForSort(blocksChunkHolder);
    
    //applyFilter(blocksChunkHolder);
    return true;
  }

  
  @Override public void readBlocklet(BlocksChunkHolder blocksChunkHolder) throws IOException {
 /*   long startTime = System.currentTimeMillis();
    this.filterExecuter.readBlocks(blocksChunkHolder);
    // adding statistics for carbon read time
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + (System.currentTimeMillis() - startTime));
    queryStatisticsModel.getRecorder().recordStatistics(readTime);*/
    
    throw new RuntimeException("no support");
  }

  /**
   * This method will process the data in below order
   * 1. first apply min max on the filter tree and check whether any of the filter
   * is fall on the range of min max, if not then return empty result
   * 2. If filter falls on min max range then apply filter on actual
   * data and get the filtered row index
   * 3. if row index is empty then return the empty result
   * 4. if row indexes is not empty then read only those blocks(measure or dimension)
   * which was present in the query but not present in the filter, as while applying filter
   * some of the blocks where already read and present in chunk holder so not need to
   * read those blocks again, this is to avoid reading of same blocks which was already read
   * 5. Set the blocks and filter indexes to result
   *
   * @param blocksChunkHolder
   * @throws FilterUnsupportedException
   */
  @Override
  public boolean applyFilter(BlocksChunkHolder blocksChunkHolder)
      throws FilterUnsupportedException, IOException {
    // long startTime = System.currentTimeMillis();
    // apply filter on actual data
    // TODO
    BitSetGroup bitSetGroup = this.filterExecuter.applyFilter(blocksChunkHolder);
    // if indexes is empty then return with empty result
    if (bitSetGroup.isEmpty()) {
      CarbonUtil.freeMemory(blocksChunkHolder.getDimensionRawDataChunk(),
          blocksChunkHolder.getMeasureRawDataChunk());
      return false;//createEmptyResult();
    }

    
    AbstractScannedSortResult scannedResult = new FilterQueryScannedSortResult(blockExecutionInfo);
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
            .getDataBlock().nodeNumber());
    // valid scanned blocklet
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(validScannedBlockletStatistic);
    // adding statistics for valid number of pages
    QueryStatistic validPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_PAGE_SCANNED);
    validPages.addCountStatistic(QueryStatisticsConstants.VALID_PAGE_SCANNED,
        validPages.getCount() + bitSetGroup.getValidPages());
    queryStatisticsModel.getRecorder().recordStatistics(validPages);
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(totalBlockletStatistic);
    
    
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        blocksChunkHolder.getDimensionRawDataChunk();
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][];
    int sortIndex = blocksChunkHolder.getAllSortDimensionBlocksIndexes()[0];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
      }
    }
    
    //blocksChunkHolder.getDimensionRawDataChunk()[blocksChunkHolder.getAllSortDimensionBlocksIndexes()[0]].convertToDimColDataChunks();
    int[] rowCount = new int[bitSetGroup.getNumberOfPages()];
    // get the row indexes from bot set
    int[][] indexesGroup = new int[bitSetGroup.getNumberOfPages()][];
    int[][] physicalIndexesGroup = new int[bitSetGroup.getNumberOfPages()][];
    //List<Integer> validPageList =  new ArrayList<Integer>(indexesGroup.length);
    int validPageCnt = 0;
    for (int k = 0; k < indexesGroup.length; k++) {
      BitSet bitSet = bitSetGroup.getBitSet(k);
      if (bitSet != null && !bitSet.isEmpty()) {
        int[] indexes = new int[bitSet.cardinality()];
        int[] physicalIndexes = new int[indexes.length];
        int index = 0;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          physicalIndexes[index] = dimensionColumnDataChunks[sortIndex][k].getInvertedIndexReverse(i);
          indexes[index++] = i;
         
        }
        
        if(indexes.length > 0 ){
          //validPageList.add(k);
          validPageCnt ++;
          rowCount[k] = indexes.length;
        }
        indexesGroup[k] = indexes;
        long start = System.currentTimeMillis();
        Arrays.sort(physicalIndexes);
        System.out.println("physicalIndexes.length: "+physicalIndexes.length+" sort time: " + (System.currentTimeMillis()-start));
        physicalIndexesGroup[k] = physicalIndexes;
      }
    }
    scannedResult.setValidPageCnt(validPageCnt);
    scannedResult.setIndexes(indexesGroup);
    scannedResult.setPhysicalRowMapping(physicalIndexesGroup);
    scannedResult.setNumberOfRows(rowCount);
    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
    scannedResult.setFilterQueryFlg(true);
    
    blocksChunkHolder.setNodeSizeForFilterQuery(scannedResult.numberOfOutputRows());
    blocksChunkHolder.setScannedResult(scannedResult);
    return true;
    /*    // loading delete data cache in blockexecutioninfo instance
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
    *//**
     * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
     * then loading them
     *//*
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
    *//**
     * in case projection if the projected measure are not loaded in the measureColumnDataChunk
     * then loading them
     *//*
    int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
    int projectionListMeasureIndexesLength = projectionListMeasureIndexes.length;
    for (int i = 0; i < projectionListMeasureIndexesLength; i++) {
      if (null == measureRawColumnChunks[projectionListMeasureIndexes[i]]) {
        measureRawColumnChunks[projectionListMeasureIndexes[i]] = blocksChunkHolder.getDataBlock()
            .getMeasureChunk(fileReader, projectionListMeasureIndexes[i]);
      }
    }
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][indexesGroup.length];
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureRawColumnChunks.length][indexesGroup.length];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      for (int j = 0; j < indexesGroup.length; j++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i][j] = dimensionRawColumnChunks[i].convertToDimColDataChunk(j);
        }
      }
    }
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      for (int j = 0; j < indexesGroup.length; j++) {
        if (measureRawColumnChunks[i] != null) {
          measureColumnDataChunks[i][j] = measureRawColumnChunks[i].convertToMeasureColDataChunk(j);
        }
      }
    }*/
    /*    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
    scannedResult.setIndexes(indexesGroup);
    scannedResult.setMeasureChunks(measureColumnDataChunks);
    scannedResult.setRawColumnChunks(dimensionRawColumnChunks);
    scannedResult.setNumberOfRows(rowCount);
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime));
    queryStatisticsModel.getRecorder().recordStatistics(scanTime);*/

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

    AbstractScannedSortResult scannedResult = blocksChunkHolder.getScannedResult();
    
    
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        blocksChunkHolder.getDimensionRawDataChunk();
    DimensionColumnDataChunk[][] dimensionColumnDataChunks = scannedResult.getDimensionChunks();
        //new DimensionColumnDataChunk[dimensionRawColumnChunks.length][];
/*    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
      }
    }*/

    int[][] measureIndexRange = blocksChunkHolder.getBlockExecutionInfo().getAllSelectedMeasureBlocksIndexes();
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureIndexRange[measureIndexRange.length-1][1] + 1][];
    
    int validPageCnt = scannedResult.getValidPageCnt();
    AbstractScannedSortResult[] scannedResults = new AbstractScannedSortResult[validPageCnt];
    scannedResults[0] = scannedResult;
    for (int i = 1; i < validPageCnt; i++) {
      scannedResults[i] = new FilterQueryScannedSortResult(blockExecutionInfo);
    }
    
    transferToScanResultArr(blocksChunkHolder, orderType, dimensionRawColumnChunks,
        dimensionColumnDataChunks, measureColumnDataChunks, scannedResults);

    return scannedResults;
  }
}

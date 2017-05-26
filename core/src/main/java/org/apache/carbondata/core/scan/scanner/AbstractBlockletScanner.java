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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.mutate.data.BlockletDeleteDeltaCacheLoader;
import org.apache.carbondata.core.mutate.data.DeleteDeltaCacheLoaderIntf;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Blocklet scanner class to process the block
 */
public abstract class AbstractBlockletScanner implements BlockletScanner {

  /**
   * block execution info
   */
  protected BlockExecutionInfo blockExecutionInfo;

  public QueryStatisticsModel queryStatisticsModel;

  private AbstractScannedResult emptyResult;
  protected ExecutorService executorService;

  public AbstractBlockletScanner(BlockExecutionInfo tableBlockExecutionInfos,
      ExecutorService executorService) {
    this.blockExecutionInfo = tableBlockExecutionInfos;
    this.executorService = executorService;
  }

  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, FilterUnsupportedException {
    long startTime = System.currentTimeMillis();
    AbstractScannedResult scannedResult = new NonFilterQueryScannedResult(blockExecutionInfo);
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    // adding statistics for valid number of pages
    QueryStatistic validPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_PAGE_SCANNED);
    validPages.addCountStatistic(QueryStatisticsConstants.VALID_PAGE_SCANNED,
        validPages.getCount() + blocksChunkHolder.getDataBlock().numberOfPages());
    // adding statistics for number of pages
    QueryStatistic totalPagesScanned = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_PAGE_SCANNED);
    totalPagesScanned.addCountStatistic(QueryStatisticsConstants.TOTAL_PAGE_SCANNED,
        totalPagesScanned.getCount() + blocksChunkHolder.getDataBlock().numberOfPages());
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
            .getDataBlock().nodeNumber());
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        blocksChunkHolder.getDimensionRawDataChunk();
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][];

    /*    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
      }
    }
    scannedResult.setDimensionChunks(dimensionColumnDataChunks);*/
    MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getMeasureRawDataChunk();
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureRawColumnChunks.length][];
    /*    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      if (measureRawColumnChunks[i] != null) {
        measureColumnDataChunks[i] = measureRawColumnChunks[i].convertToMeasureColDataChunks();
      }
    }
    scannedResult.setMeasureChunks(measureColumnDataChunks);*/

    convertToColDataChunk(0, dimensionRawColumnChunks, measureRawColumnChunks,
        dimensionColumnDataChunks, measureColumnDataChunks);
    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
    scannedResult.setMeasureChunks(measureColumnDataChunks);

    int[] numberOfRows = new int[] { blocksChunkHolder.getDataBlock().nodeSize() };
    if (blockExecutionInfo.getAllSelectedDimensionBlocksIndexes().length > 0) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          numberOfRows = dimensionRawColumnChunks[i].getRowCount();
          break;
        }
      }
    } else if (blockExecutionInfo.getAllSelectedMeasureBlocksIndexes().length > 0) {
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (measureRawColumnChunks[i] != null) {
          numberOfRows = measureRawColumnChunks[i].getRowCount();
          break;
        }
      }
    }
    scannedResult.setNumberOfRows(numberOfRows);
    // loading delete data cache in blockexecutioninfo instance
    DeleteDeltaCacheLoaderIntf deleteCacheLoader =
        new BlockletDeleteDeltaCacheLoader(scannedResult.getBlockletId(),
            blocksChunkHolder.getDataBlock(), blockExecutionInfo.getAbsoluteTableIdentifier());
    deleteCacheLoader.loadDeleteDeltaFileDataToCache();
    scannedResult
        .setBlockletDeleteDeltaCache(blocksChunkHolder.getDataBlock().getDeleteDeltaDataCache());
    scannedResult.setRawColumnChunks(dimensionRawColumnChunks);
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime));
    return scannedResult;
  }

  @Override public void readBlocklet(BlocksChunkHolder blocksChunkHolder) throws IOException {
    long startTime = System.currentTimeMillis();
    DimensionRawColumnChunk[] dimensionRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes());
    blocksChunkHolder.setDimensionRawDataChunk(dimensionRawColumnChunks);
    MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getMeasureChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedMeasureBlocksIndexes());
    blocksChunkHolder.setMeasureRawDataChunk(measureRawColumnChunks);
    // adding statistics for carbon read time
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  @Override public AbstractScannedResult createEmptyResult() {
    if (emptyResult == null) {
      emptyResult = new NonFilterQueryScannedResult(blockExecutionInfo);
      emptyResult.setNumberOfRows(new int[0]);
      emptyResult.setIndexes(new int[0][]);
    }
    return emptyResult;
  }

  @Override public boolean isScanRequired(BlocksChunkHolder blocksChunkHolder) throws IOException {
    // For non filter it is always true
    return true;
  }

  protected class CarbonDimCallable implements Callable<DimensionColumnDataChunk[]> {
    DimensionRawColumnChunk rawDim;
    int numberOfPages;

    public CarbonDimCallable(DimensionRawColumnChunk rawDim, int numberOfPages) {
      this.rawDim = rawDim;
      this.numberOfPages = numberOfPages == 0 ? rawDim.getPagesCount() : numberOfPages;
    }

    @Override
    public DimensionColumnDataChunk[] call() throws Exception {

      DimensionColumnDataChunk[] dims = new DimensionColumnDataChunk[numberOfPages];
      for (int j = 0; j < numberOfPages; j++) {

        dims[j] = rawDim.convertToDimColDataChunk(j);

      }
      return dims;
    }
  }

  protected class CarbonMeasureCallable implements Callable<MeasureColumnDataChunk[]> {
    MeasureRawColumnChunk rawMeasure;
    int numberOfPages;

    public CarbonMeasureCallable(MeasureRawColumnChunk rawMeasure, int numberOfPages) {
      this.rawMeasure = rawMeasure;
      this.numberOfPages = numberOfPages == 0 ? rawMeasure.getPagesCount() : numberOfPages;
    }

    @Override
    public MeasureColumnDataChunk[] call() throws Exception {

      MeasureColumnDataChunk[] measures = new MeasureColumnDataChunk[numberOfPages];
      for (int j = 0; j < numberOfPages; j++) {
        measures[j] = rawMeasure.convertToMeasureColDataChunk(j);
      }
      return measures;
    }
  }
  public void convertToColDataChunk(int numberOfPages,
      DimensionRawColumnChunk[] dimensionRawColumnChunks,
      MeasureRawColumnChunk[] measureRawColumnChunks,
      DimensionColumnDataChunk[][] dimensionColumnDataChunks,
      MeasureColumnDataChunk[][] measureColumnDataChunks) throws IOException {
    List<Future<?>> futureList = new ArrayList<Future<?>>(
        dimensionRawColumnChunks.length + measureRawColumnChunks.length);
    // ExecutorService executorService = Executors.newCachedThreadPool();
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        futureList.add(executorService
            .submit(new CarbonDimCallable(dimensionRawColumnChunks[i], numberOfPages)));
      }
    }
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      if (measureRawColumnChunks[i] != null) {
        futureList.add(executorService
            .submit(new CarbonMeasureCallable(measureRawColumnChunks[i], numberOfPages)));
      }
    }

    try {
      int cnt = 0;
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i] = (DimensionColumnDataChunk[]) futureList.get(cnt).get();
          cnt++;
        }
      }
      for (int i = 0; i < measureRawColumnChunks.length; i++) {

        if (measureRawColumnChunks[i] != null) {
          measureColumnDataChunks[i] = (MeasureColumnDataChunk[]) futureList.get(cnt).get();
          cnt++;
        }
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

    /* if (numberOfPages > 0) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        for (int j = 0; j < numberOfPages; j++) {
          if (dimensionRawColumnChunks[i] != null) {
            dimensionColumnDataChunks[i][j] = dimensionRawColumnChunks[i]
                .convertToDimColDataChunk(j);
          }
        }
      }
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        for (int j = 0; j < numberOfPages; j++) {
          if (measureRawColumnChunks[i] != null) {
            measureColumnDataChunks[i][j] = measureRawColumnChunks[i]
                .convertToMeasureColDataChunk(j);
          }
        }
      }
    } else {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
        }
      }
      for (int j = 0; j < measureRawColumnChunks.length; j++) {
        if (measureRawColumnChunks[j] != null) {
          measureColumnDataChunks[j] = measureRawColumnChunks[j].convertToMeasureColDataChunks();
        }
      }
    }*/
  }
}

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
package org.apache.carbondata.core.scan.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.result.ScanResultComparator;
import org.apache.carbondata.core.scan.scanner.impl.FilterSortScanner;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterSortScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * This abstract class provides a skeletal implementation of the Block iterator.
 */
public class CaculateDataBlocksToScan {

  private static final LogService LOGGER = LogServiceFactory
      .getLogService(CaculateDataBlocksToScan.class.getName());

  // private List<Future<AbstractSortScannedResult>> scanResultfutureList = new
  // ArrayList<Future<AbstractSortScannedResult>>();

  /**
   * result collector which will be used to aggregate the scanned result
   */

  /**
   * processor which will be used to process the block processing can be filter
   * processing or non filter processing
   */
  // protected BlockletScanner blockletScanner;

  /**
   * to hold the data block
   */
  // protected BlocksChunkHolder blocksChunkHolder;

  QueryStatisticsModel queryStatisticsModel;

  public static void caculateDataBlocksToScan(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel,
      LimitFilteredBlocksChunkHolders blocksChunkHolderLimitFilter) throws QueryExecutionException {

    DataRefNode firstDataBlock = blockExecutionInfo.getFirstDataBlock();

    /*
     * blocksChunkHolderLimitFilter = new
     * BlocksChunkHolderLimitFilter(blockExecutionInfo.getLimit(),
     * blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0],
     * queryModel.getSortDimensions().get(0).getSortOrder());
     */

    blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(blockExecutionInfo,
        fileReader, queryStatisticsModel, queryModel, firstDataBlock));

    /*
     * blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo,
     * fileReader, queryStatisticsModel, queryModel, firstDataBlock));
     * scanResultfutureList.add(scanBlockletForSortDimension(
     * generateBlocksChunkHolder(blockExecutionInfo, fileReader,
     * queryStatisticsModel, queryModel, firstDataBlock), execService));
     */

    firstDataBlock = firstDataBlock.getNextDataRefNode();
    while (firstDataBlock != null) {

      blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(
          blockExecutionInfo, fileReader, queryStatisticsModel, queryModel, firstDataBlock));
      // blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo,
      // fileReader, queryStatisticsModel,
      // queryModel, firstDataBlock));
      // scanResultfutureList.add(scanBlockletForSortDimension(tmpBlocksChunkHolder,
      // execService));

      firstDataBlock = firstDataBlock.getNextDataRefNode();
    }

  }

  private static BlocksChunkHolder generateBlocksChunkHolder(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel,
      DataRefNode dataBlock) throws QueryExecutionException {
    BlocksChunkHolder tmpBlocksChunkHolder;
    tmpBlocksChunkHolder = new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
        blockExecutionInfo.getTotalNumberOfMeasureBlock());
    tmpBlocksChunkHolder.setLimit(queryModel.getLimit());
    tmpBlocksChunkHolder.setAllSortDimensionBlocksIndexes(blockExecutionInfo.getAllSortDimensionBlocksIndexes());
    tmpBlocksChunkHolder.setNodeSize(dataBlock.nodeSize());
    tmpBlocksChunkHolder.setFileReader(fileReader);
    tmpBlocksChunkHolder.setDataBlock(dataBlock);
    tmpBlocksChunkHolder.setMinValueForSortKey(
        dataBlock.getColumnsMinValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
    tmpBlocksChunkHolder.setMaxValueForSortKey(
        dataBlock.getColumnsMaxValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
    // tmpBlocksChunkHolder.setNodeNumber(dataBlock.nodeNumber());
    // String path[] = dataBlock.getFilePath().split("/");
    tmpBlocksChunkHolder.setBlockletNodeId(dataBlock.getNodeId());

    tmpBlocksChunkHolder.setBlockExecutionInfo(blockExecutionInfo);
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      FilterSortScanner filter = new FilterSortScanner(blockExecutionInfo, queryStatisticsModel);
      if (filter.applyFilter(tmpBlocksChunkHolder)) {
        return null;
      }
      tmpBlocksChunkHolder.setBlockletScanner(filter);
    } else {
      tmpBlocksChunkHolder
          .setBlockletScanner(new NonFilterSortScanner(blockExecutionInfo, queryStatisticsModel));
    }
    return tmpBlocksChunkHolder;
  }

  private static Future<AbstractScannedSortResult[]> executeRead(ExecutorService execService,
      final BlocksChunkHolder blocksChunkHolder, final SortOrderType sortType) {
    return execService.submit(new Callable<AbstractScannedSortResult[]>() {
      @Override
      public AbstractScannedSortResult[] call() throws Exception {

        if (blocksChunkHolder != null) {
          blocksChunkHolder.getBlockletScanner().readBlockletForSort(blocksChunkHolder);
          return blocksChunkHolder.scanBlockletForSort(sortType);
        }

        return null;
      }
    });
  }

  // only load the sort dim data ,others will be lazy loaded
  public static TreeSet<AbstractScannedSortResult> generateAllScannedResultSet(
      ExecutorService execService,
      LimitFilteredBlocksChunkHolders blocksChunkHolderLimitFilter, SortOrderType sortType)
      throws IOException {
    List<Future<AbstractScannedSortResult[]>> scanResultfutureList = new ArrayList<Future<AbstractScannedSortResult[]>>();
    TreeSet<BlocksChunkHolder> requiredToScanBlocksChunkHolderSet = blocksChunkHolderLimitFilter
        .getRequiredToScanBlocksChunkHolderSet();
    TreeSet<AbstractScannedSortResult> scannedResultSet = new TreeSet<AbstractScannedSortResult>(
        new ScanResultComparator(sortType));
    try {

      Iterator<BlocksChunkHolder> it = requiredToScanBlocksChunkHolderSet.iterator();

      // byte[] current = blocksChunkHolderLimitFilter.getMinValueForSortKey();
      while (it.hasNext()) {
        BlocksChunkHolder blocksChunkHolder = it.next();

        scanResultfutureList
            .add(executeRead(execService, blocksChunkHolder, sortType));

      }

      // long start = System.currentTimeMillis();
      for (int i = 0; i < scanResultfutureList.size(); i++) {
        Future<AbstractScannedSortResult[]> futureResult = scanResultfutureList.get(i);

        AbstractScannedSortResult[] detailedScannedResults = futureResult.get();

        // LOGGER.info("detailedScannedResult.getCurrentSortDimentionKey()"
        // + detailedScannedResult.getCurrentSortDimentionKey());
        if (detailedScannedResults != null) {

          scannedResultSet.addAll(Arrays.asList(detailedScannedResults));

        } else {
          LOGGER.info("null result set");
        }

      }

    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return scannedResultSet;
  }



}

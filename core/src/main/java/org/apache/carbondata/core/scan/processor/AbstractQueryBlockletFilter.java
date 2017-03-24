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
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.collector.ResultCollectorFactory;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * This abstract class provides a skeletal implementation of the Block iterator.
 */
public abstract class AbstractQueryBlockletFilter {

  private static final LogService LOGGER = LogServiceFactory
      .getLogService(AbstractQueryBlockletFilter.class.getName());

  /**
   * result collector which will be used to aggregate the scanned result
   */
  protected ScannedResultCollector scannerResultAggregator;

  /**
   * processor which will be used to process the block processing can be
   * filter processing or non filter processing
   */
  protected BlockletScanner blockletScanner;
  protected ExecutorService executorService;
  protected DataRefNode firstDataBlock;

  protected BlockExecutionInfo blockExecutionInfo;

  protected FileHolder fileReader;
  protected SortOrderType sortType;

  protected QueryStatisticsModel queryStatisticsModel;

  public AbstractQueryBlockletFilter(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      QueryStatisticsModel queryStatisticsModel, ExecutorService executorService, SortOrderType sortType) {
    this.blockExecutionInfo = blockExecutionInfo;
    this.fileReader = fileReader;
    this.sortType = sortType;
    
//    if (blockExecutionInfo.getFilterExecuterTree() != null) {
//      blockletScanner = new FilterSortScanner(blockExecutionInfo, queryStatisticsModel);
//    } else {
//      blockletScanner = new NonFilterSortScanner(blockExecutionInfo, queryStatisticsModel);
//    }

    this.scannerResultAggregator =
        ResultCollectorFactory.getScannedResultCollector(blockExecutionInfo);
    this.executorService = executorService;
    firstDataBlock = blockExecutionInfo.getFirstDataBlock();

  }
  
  public abstract void filterDataBlocklets(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel,
      LimitFilteredBlocksChunkHolders blocksChunkHolderLimitFilter) throws QueryExecutionException, IOException, InterruptedException, ExecutionException, FilterUnsupportedException;
  public abstract boolean applyFilter(BlocksChunkHolder blocksChunkHolder)throws FilterUnsupportedException, IOException, FilterUnsupportedException;

  protected  BlocksChunkHolder generateBlocksChunkHolder(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, 
      DataRefNode dataBlock) throws FilterUnsupportedException,QueryExecutionException, IOException {
    BlocksChunkHolder blocksChunkHolder;
    blocksChunkHolder = new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
        blockExecutionInfo.getTotalNumberOfMeasureBlock(), fileReader);
    blocksChunkHolder.setDataBlock(dataBlock);
    blocksChunkHolder.setBlockletScanner(blockletScanner);
    blocksChunkHolder.setLimit(blockExecutionInfo.getLimit());
    blocksChunkHolder.setAllSortDimensionBlocksIndexes(blockExecutionInfo.getAllSortDimensionBlocksIndexes());
    blocksChunkHolder.setNodeSize(dataBlock.nodeSize());
    blocksChunkHolder.setMinValueForSortKey(
        dataBlock.getColumnsMinValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
    blocksChunkHolder.setMaxValueForSortKey(
        dataBlock.getColumnsMaxValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
    blocksChunkHolder.setBlockletNodeId(dataBlock.getNodeId());
    blocksChunkHolder.setBlockExecutionInfo(blockExecutionInfo);
    
    if (!blockletScanner.isScanRequired(blocksChunkHolder)) {
      return null;
    }else{
      // TODO
      
      if(!applyFilter(blocksChunkHolder)){
        return null;
      }
    }

    return blocksChunkHolder;
  }


  public Future<AbstractScannedSortResult[]> executeRead(ExecutorService execService,
      final BlocksChunkHolder blocksChunkHolder) {
    return execService.submit(new Callable<AbstractScannedSortResult[]>() {
      @Override
      public AbstractScannedSortResult[] call() throws Exception {

        if (blocksChunkHolder != null) {
          // read sort dim data
          blocksChunkHolder.readBlockletForSort();
          // transfer the data to scanned sort results
          return blocksChunkHolder.scanBlockletForSort(sortType);
        }

        return null;
      }
    });
  }

  protected static void printTreeSet(TreeSet<AbstractScannedSortResult> scannedResultSet ) {
    System.out.println("start print");
    for(AbstractScannedSortResult detailedScannedResult : scannedResultSet){
        
      System.out.println("CurrentSortDimentionKey in scannedResultSet: " + detailedScannedResult.getCurrentSortDimentionKey());  
    }
    System.out.println("end print");
}

}

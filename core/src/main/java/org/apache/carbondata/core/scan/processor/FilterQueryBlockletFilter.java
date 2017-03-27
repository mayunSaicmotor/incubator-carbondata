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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.scanner.impl.FilterSortScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * This abstract class provides a skeletal implementation of the Block iterator.
 */
public class FilterQueryBlockletFilter extends AbstractQueryBlockletFilter {

  private static final LogService LOGGER = LogServiceFactory
      .getLogService(FilterQueryBlockletFilter.class.getName());



  public FilterQueryBlockletFilter(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      QueryStatisticsModel queryStatisticsModel, ExecutorService executorService, SortOrderType sortType) {
    super(blockExecutionInfo, fileReader, queryStatisticsModel, executorService, sortType);
    blockletScanner = new FilterSortScanner(blockExecutionInfo, queryStatisticsModel);

  }
  
  //No filter
  @Override
  public boolean applyFilter(BlocksChunkHolder blocksChunkHolder) throws FilterUnsupportedException, IOException{
    return blockletScanner.applyFilter(blocksChunkHolder);
  }
  
  @Override
  public  void filterDataBlocklets(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel,
      LimitFilteredBlocksChunkHolders blocksChunkHolderLimitFilter) throws QueryExecutionException, IOException, InterruptedException, ExecutionException {

//    firstDataBlock = blockExecutionInfo.getFirstDataBlock();
//    blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(blockExecutionInfo,
//        fileReader, queryStatisticsModel, firstDataBlock));
//    firstDataBlock = firstDataBlock.getNextDataRefNode();
    
    List<Future<BlocksChunkHolder>> blocksChunkHolderFuture = new ArrayList<Future<BlocksChunkHolder>>();
    while (firstDataBlock != null) {

      blocksChunkHolderFuture.add(executeReadFilterAndSortColumnData(firstDataBlock));
//      blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(
//          blockExecutionInfo, fileReader, queryStatisticsModel, firstDataBlock));
      // blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo,
      // fileReader, queryStatisticsModel,
      // queryModel, firstDataBlock));
      // scanResultfutureList.add(scanBlockletForSortDimension(tmpBlocksChunkHolder,
      // execService));

      firstDataBlock = firstDataBlock.getNextDataRefNode();
    }

    for(Future<BlocksChunkHolder> bchFuture : blocksChunkHolderFuture){
      
      blocksChunkHolderLimitFilter.addBlocksChunkHolder(bchFuture.get());
      
    }

  }

  @Override
  public Future<AbstractScannedSortResult[]> executeRead(ExecutorService execService,
      final BlocksChunkHolder blocksChunkHolder) {
    return execService.submit(new Callable<AbstractScannedSortResult[]>() {
      @Override
      public AbstractScannedSortResult[] call() throws Exception {

        if (blocksChunkHolder != null) {
          blocksChunkHolder.readBlockletForSort();
          return blocksChunkHolder.scanBlockletForSort(sortType);
        }

        return null;
      }
    });
  }
  protected Future<BlocksChunkHolder> executeReadFilterAndSortColumnData( final DataRefNode dataBlock) {
    return executorService.submit(new Callable<BlocksChunkHolder>() {
      @Override public BlocksChunkHolder call() throws Exception {
          BlocksChunkHolder blocksChunkHolder = generateBlocksChunkHolder(blockExecutionInfo,
              fileReader, queryStatisticsModel, dataBlock);
          if (blocksChunkHolder != null) {
            // blockletScanner.readBlocklet(blocksChunkHolder);
            return blocksChunkHolder;
          }

        return null;
      }
    });
  }


}

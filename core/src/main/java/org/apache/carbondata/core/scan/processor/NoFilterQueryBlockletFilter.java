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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterSortScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * This abstract class provides a skeletal implementation of the Block iterator.
 */
public class NoFilterQueryBlockletFilter extends  AbstractQueryBlockletFilter{

  private static final LogService LOGGER = LogServiceFactory
      .getLogService(NoFilterQueryBlockletFilter.class.getName());



  public NoFilterQueryBlockletFilter(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      QueryStatisticsModel queryStatisticsModel, ExecutorService executorService, SortOrderType sortType) {
    
    super(blockExecutionInfo, fileReader, queryStatisticsModel, executorService, sortType);
    
    blockletScanner = new NonFilterSortScanner(blockExecutionInfo, queryStatisticsModel);
    

  }
  
  //No filter
  @Override
  public  boolean applyFilter(BlocksChunkHolder blocksChunkHolder) throws FilterUnsupportedException, IOException{
    return true;
  }
  @Override
  public  void filterDataBlocklets(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel,
      LimitFilteredBlocksChunkHolders blocksChunkHolderLimitFilter) throws QueryExecutionException, IOException, InterruptedException, ExecutionException, FilterUnsupportedException {

//    firstDataBlock = blockExecutionInfo.getFirstDataBlock();
//    blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(blockExecutionInfo,
//        fileReader, queryStatisticsModel, firstDataBlock));
//    firstDataBlock = firstDataBlock.getNextDataRefNode();
    
    while (firstDataBlock != null) {

      blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(
          blockExecutionInfo, fileReader, queryStatisticsModel, firstDataBlock));
      // blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo,
      // fileReader, queryStatisticsModel,
      // queryModel, firstDataBlock));
      // scanResultfutureList.add(scanBlockletForSortDimension(tmpBlocksChunkHolder,
      // execService));

      firstDataBlock = firstDataBlock.getNextDataRefNode();
    }

  }


}

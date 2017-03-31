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
package org.apache.carbondata.core.scan.executor.impl;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.iterator.DetailQueryResultIterator;
import org.apache.carbondata.core.scan.result.iterator.DetailQuerySortByMdkResultIterator;
import org.apache.carbondata.core.scan.result.iterator.DetailQuerySortResultIterator;

/**
 * Below class will be used to execute the detail query
 * For executing the detail query it will pass all the block execution
 * info to detail query result iterator and iterator will be returned
 */
public class DetailQueryExecutor extends AbstractQueryExecutor<BatchResult> {

  @Override
  public CarbonIterator<BatchResult> execute(QueryModel queryModel)
      throws QueryExecutionException, IOException {
    List<BlockExecutionInfo> blockExecutionInfoList = getBlockExecutionInfos(queryModel);
    //TODO for sort, if blockExecutionInfoList.size >1, it can't make sure the data's order in 2 block
    if (blockExecutionInfoList.get(0).isSortFlg()) {
      if(blockExecutionInfoList.size() == 1){
        if(blockExecutionInfoList.get(0).isOrderByPrefixMdkFlg()){
          return new DetailQuerySortByMdkResultIterator(
              blockExecutionInfoList,
              queryModel,
              queryProperties.executorService
          );
        }
        return new DetailQuerySortResultIterator(blockExecutionInfoList, queryModel,
            queryProperties.executorService);
      }else{
         throw new UnsupportedOperationException("if process multi block executions, it can't guarantee the result is sorted");
      }

    }
    this.queryIterator = new DetailQueryResultIterator(
        blockExecutionInfoList,
        queryModel,
        queryProperties.executorService
    );
    return queryIterator;
  }

}

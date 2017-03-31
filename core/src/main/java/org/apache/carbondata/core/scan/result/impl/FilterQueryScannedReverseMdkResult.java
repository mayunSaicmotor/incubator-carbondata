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
package org.apache.carbondata.core.scan.result.impl;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedReverseMdkResult extends FilterQueryScannedResult {

  public FilterQueryScannedReverseMdkResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
    --currentRow;
    return getDictionaryKeyArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    --currentRow;
    return getDictionaryKeyIntegerArray(rowMapping[pageCounter][currentRow]);
  }
  /**
   * to check whether any more row is present in the result
   *
   * @return
   */
  @Override public boolean hasNext() {
    if (pageCounter < numberOfRows.length && rowCounter < this.numberOfRows[pageCounter]) {
      return true;
    } else if (pageCounter > 0) {
      pageCounter--;
      rowCounter = 0;
      currentRow = this.numberOfRows[pageCounter];
      return hasNext();
    }
    return false;
  }
  
  @Override
  public void setIndexes(int[][] indexes) {
    super.setIndexes(indexes);
    pageCounter = indexes.length - 1;
    currentRow = indexes[pageCounter].length;
  }
  
}

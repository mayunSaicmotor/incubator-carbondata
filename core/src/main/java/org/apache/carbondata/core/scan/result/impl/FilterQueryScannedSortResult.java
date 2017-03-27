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
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedSortResult extends AbstractScannedSortResult {

  public FilterQueryScannedSortResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
   // ++currentRow;
   // return getDictionaryKeyArray(rowMapping[pageCounter][this.currentLogicRowId]);
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    // return getDictionaryKeyIntegerArray(rowMapping[pageCounter][this.currentLogicRowId]);
    return getDictionaryKeyIntegerArray(currentRow);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(this.currentLogicRowId);
  }

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(this.currentLogicRowId);
  }

  /**
   * Below method will be used to get the no dictionary key
   * string array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public String[] getNoDictionaryKeyStringArray() {
    return getNoDictionaryKeyStringArray(this.currentLogicRowId);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrentRowId() {
    //return rowMapping[pageCounter][currentRow];
    return currentLogicRowId;
  }
@Override
public void caculateCurrentRowId(int rowId) {
    
    currentPhysicalRowId = rowId;
    //for sort query push down sort
    //if(sortSingleDimensionBlocksIndex >= 0){
        
        //baseSortDimentionInvertedIndexes = dataChunks[sortSingleDimensionBlocksIndex].getAttributes().getInvertedIndexes();
        
    if (baseSortDimentionDataChunk.isExplicitSorted()) {

      // for filter query
      // if(this.rowMapping != null && this.rowMapping[pageCounter] != null&&
      // this.rowMapping[pageCounter].length > 0){
      if (this.filterQueryFlg) {

        if (this.descSortFlg) {
          currentPhysicalRowId = this.physicalRowMapping[pageCounter].length - rowId - 1;
          this.currentLogicRowId = baseSortDimentionDataChunk
              .getInvertedIndex(this.physicalRowMapping[pageCounter][currentPhysicalRowId]);
        } else {
          this.currentLogicRowId = baseSortDimentionDataChunk.getInvertedIndex(this.physicalRowMapping[pageCounter][rowId]);
        }
      }

      // when baseSortDimentionInvertedIndexes = null
    } else {

      caculateCurrentRowIdForNoInvertedIndexFilterQuery();
    }
        
    //for no sort query
    //}
}

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
}

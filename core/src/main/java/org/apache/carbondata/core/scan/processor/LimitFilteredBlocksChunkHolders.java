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

import java.util.TreeSet;

import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.scan.model.SortOrderType;

/**
 * Block chunk holder which will hold the dimension and
 * measure chunk
 */
public class LimitFilteredBlocksChunkHolders {

	  private SortOrderType sortType;
	  private int limit;
	  private int totalRowNumber=0;
	  private int sortDimensionIndex;
	  private byte[] maxValueForSortKey = null;
	  private byte[] minValueForSortKey = null;
	  //private  List<BlocksChunkHolder> blocksChunkHolderList = new ArrayList<BlocksChunkHolder>();
	  private TreeSet<BlocksChunkHolder> requiredToScanBlocksChunkHolderSet = null;
	  public LimitFilteredBlocksChunkHolders(int limit, int sortDimensionIndex, SortOrderType sortType){
		  this.limit = limit;
		  this.sortDimensionIndex = sortDimensionIndex;
		  this.sortType = sortType;
		  
	  }

	  
	  public void addBlocksChunkHolder(BlocksChunkHolder blocksChunkHolder){
		  
		  //System.out.println("blocksChunkHolder.getMinValueForSortKey(): "+blocksChunkHolder.getMinValueForSortKey());
		  if(blocksChunkHolder == null){
			  return;
		  }
		 
			
		  if(minValueForSortKey == null || maxValueForSortKey == null){
			  
			  initAdd(blocksChunkHolder);
			  return;
		  }else{
			  
			  int tmpTotalRowNumber = caculateValidTotalRowNumber(blocksChunkHolder);
			  
			  //  ASC order
			  if(SortOrderType.ASC.equals(sortType)){
				  
				int compare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMinValueForSortKey(), maxValueForSortKey);
				  //exceed the max value

				if (compare > 0 && this.totalRowNumber > limit) {
					//System.out.println("filter 1 blocklet");
					return;
				}
				
				compare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMaxValueForSortKey(), minValueForSortKey);			
				// re-new the set
				if (compare < 0 && tmpTotalRowNumber >= limit) {

					//System.out.println("re-init the block set");
					initAdd(blocksChunkHolder);
					return;
				}

				addNext(blocksChunkHolder);
			
			  //Desc order
			  }else{

					int compare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMaxValueForSortKey(), minValueForSortKey);
					  //exceed the max value

					if (compare < 0 && this.totalRowNumber > limit) {
						//System.out.println("filter 1 blocklet");
					  CarbonUtil.freeMemory(blocksChunkHolder.getDimensionRawDataChunk(),
				          blocksChunkHolder.getMeasureRawDataChunk());
						return;
					}
					
					compare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMinValueForSortKey(), maxValueForSortKey);			
					// re-new the set
					if (compare > 0 && tmpTotalRowNumber >= limit) {

					    freeMemoryForFilteredBlocks();
                        //System.out.println("re-init the block set");
						initAdd(blocksChunkHolder);
						return;
					}

					addNext(blocksChunkHolder);
				  
			  }
			  
		  }

	  }


    public void freeMemoryForFilteredBlocks() {
      for(BlocksChunkHolder b : requiredToScanBlocksChunkHolderSet){
                 CarbonUtil.freeMemory(b.getDimensionRawDataChunk(),
                      b.getMeasureRawDataChunk());
      }
    }


	private void addNext(BlocksChunkHolder blocksChunkHolder) {
		int minCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMinValueForSortKey(),
				minValueForSortKey);
		if (minCompare < 0) {
			this.minValueForSortKey = blocksChunkHolder.getMinValueForSortKey();
		}
		int maxCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(blocksChunkHolder.getMaxValueForSortKey(),
				maxValueForSortKey);
		if (maxCompare > 0) {
			this.maxValueForSortKey = blocksChunkHolder.getMaxValueForSortKey();
		}
		requiredToScanBlocksChunkHolderSet.add(blocksChunkHolder);
		totalRowNumber += caculateValidTotalRowNumber(blocksChunkHolder);
		
		//printByteArray(blocksChunkHolder.getMinValueForSortKey(),requiredToScanBlocksChunkHolderSet);
	}


	private void initAdd(BlocksChunkHolder blocksChunkHolder) {
		requiredToScanBlocksChunkHolderSet = new TreeSet<BlocksChunkHolder>(new BlocksChunkHolderComparator(sortType));
			requiredToScanBlocksChunkHolderSet.add(blocksChunkHolder);
		  this.maxValueForSortKey = blocksChunkHolder.getMaxValueForSortKey();
		  this.minValueForSortKey = blocksChunkHolder.getMinValueForSortKey();
		  
		  totalRowNumber = caculateValidTotalRowNumber(blocksChunkHolder);
		  
	}


	public int caculateValidTotalRowNumber(BlocksChunkHolder blocksChunkHolder) {
		//System.out.println("start to initAdd ");
		  //printByteArray(blocksChunkHolder.getMinValueForSortKey(), requiredToScanBlocksChunkHolderSet);
		  if (blocksChunkHolder.getBlockExecutionInfo().getFilterExecuterTree() != null) {
		      // TODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODOTODO
			  // return blocksChunkHolder.getBlockletScanner().getScannedResultAfterProcessFilter().numberOfOutputRows();
			  return blocksChunkHolder.getFilteredSize();
		  }else{
			  return blocksChunkHolder.getNodeSize(); 
		  }
	}


	public static synchronized void printByteArray(byte[] bytes, TreeSet<BlocksChunkHolder> requiredToScanBlocksChunkHolderSet) {
		int dict=0;
		for (int i = 0; i < bytes.length; i++) {
		    dict <<= 8;
		    dict ^= bytes[i] & 0xFF;
		  }
		//System.out.println("blocksChunkHolder min value: " + dict + " Thread.currentThread().getId(): "+Thread.currentThread().getId());
		for (final BlocksChunkHolder blocksChunkHolder : requiredToScanBlocksChunkHolderSet) {
			
			 dict=0;
		    for (int i = 0; i < blocksChunkHolder.getMinValueForSortKey().length; i++) {
		        dict <<= 8;
		        dict ^= blocksChunkHolder.getMinValueForSortKey()[i] & 0xFF;
		      }
		    
			//System.out.print(" " + dict);

		}
		//System.out.println(" Thread.currentThread().getId(): "+Thread.currentThread().getId());
		
	}


	public SortOrderType getSortType() {
		return sortType;
	}


	public void setSortType(SortOrderType sortType) {
		this.sortType = sortType;
	}


	public int getLimit() {
		return limit;
	}


	public void setLimit(int limit) {
		this.limit = limit;
	}


	public int getSortDimensionIndex() {
		return sortDimensionIndex;
	}


	public void setSortDimensionIndex(int sortDimensionIndex) {
		this.sortDimensionIndex = sortDimensionIndex;
	}


	public byte[] getMaxValueForSortKey() {
		return maxValueForSortKey;
	}


	public void setMaxValueForSortKey(byte[] maxValueForSortKey) {
		this.maxValueForSortKey = maxValueForSortKey;
	}


	public byte[] getMinValueForSortKey() {
		return minValueForSortKey;
	}


	public void setMinValueForSortKey(byte[] minValueForSortKey) {
		this.minValueForSortKey = minValueForSortKey;
	}


	public int getTotalRowNumber() {
		return totalRowNumber;
	}


	public void setTotalRowNumber(int totalRowNumber) {
		this.totalRowNumber = totalRowNumber;
	}


	public TreeSet<BlocksChunkHolder> getRequiredToScanBlocksChunkHolderSet() {
		if(requiredToScanBlocksChunkHolderSet == null){
			return new TreeSet<BlocksChunkHolder>(new BlocksChunkHolderComparator(sortType)); 
		}
		return requiredToScanBlocksChunkHolderSet;
	}


	public void setRequiredToScanBlocksChunkHolderSet(TreeSet<BlocksChunkHolder> requiredToScanBlocksChunkHolderSet) {
		this.requiredToScanBlocksChunkHolderSet = requiredToScanBlocksChunkHolderSet;
	}
	  
}

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

import java.util.Comparator;

import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.scan.model.SortOrderType;

/**
 * Result interface for storing the result
 */

	public class BlocksChunkHolderComparator implements Comparator<BlocksChunkHolder> {

		private SortOrderType sortType;
		public BlocksChunkHolderComparator(SortOrderType sortType){
			this.sortType = sortType;
		}
		public BlocksChunkHolderComparator(){
			this.sortType = SortOrderType.ASC;
		}
		
		@Override
		public int compare(BlocksChunkHolder o1, BlocksChunkHolder o2) {
			// TODO Auto-generated method stub
			
			if(SortOrderType.ASC.equals(sortType)){
				
				int minCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(o1.getMinValueForSortKey(), o2.getMinValueForSortKey());
				        		  
				if(minCompare == 0){
					return o1.getBlockletNodeId().compareTo(o2.getBlockletNodeId());
				}
				return minCompare;
			}else{
				
				int maxCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(o2.getMaxValueForSortKey(), o2.getMaxValueForSortKey());
      		  
				if(maxCompare == 0){
					return o2.getBlockletNodeId().compareTo(o1.getBlockletNodeId());
				}
				return maxCompare;
			}
			
		}
	}


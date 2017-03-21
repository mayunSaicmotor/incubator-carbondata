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

package org.apache.carbondata.core.scan.result;

import java.util.Comparator;

import org.apache.carbondata.core.scan.model.SortOrderType;

/**
 * Result interface for storing the result
 */

	public class ScanResultComparator implements Comparator<AbstractScannedSortResult> {

		private SortOrderType sortType;
		public ScanResultComparator(SortOrderType sortType){
			this.sortType = sortType;
		}
		public ScanResultComparator(){
			this.sortType = SortOrderType.ASC;
		}
		
		@Override
		public int compare(AbstractScannedSortResult o1, AbstractScannedSortResult o2) {
			// TODO Auto-generated method stub
			
			if(SortOrderType.ASC.equals(sortType)){
				
				
			int result = o1.isSortByDictionaryDimensionFlg()
					? Integer.parseInt(o1.currentSortDimentionKey) - Integer.parseInt(o2.currentSortDimentionKey)
					: o1.currentSortDimentionKey.compareTo(o2.currentSortDimentionKey);
			if (result == 0) {
					//TODO whether carbon data file name is unique,  result = o1.getNodeNumber().compareTo(o2.getNodeNumber());
					 return o1.getBlockletId().compareTo(o2.getBlockletId());
				}
				return result;
			}else{
				
			int result = o1.isSortByDictionaryDimensionFlg()
					? Integer.parseInt(o2.currentSortDimentionKey) - Integer.parseInt(o1.currentSortDimentionKey)
					: o2.currentSortDimentionKey.compareTo(o1.currentSortDimentionKey);
				
				if(result == 0){
					return o2.getBlockletId().compareTo(o1.getBlockletId());
				}
				return result;
			}
			
		}
	}


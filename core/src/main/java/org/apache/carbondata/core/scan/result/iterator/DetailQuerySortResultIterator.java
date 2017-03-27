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
package org.apache.carbondata.core.scan.result.iterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.AbstractQueryBlockletFilter;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.processor.FilterQueryBlockletFilter;
import org.apache.carbondata.core.scan.processor.LimitFilteredBlocksChunkHolders;
import org.apache.carbondata.core.scan.processor.NoFilterQueryBlockletFilter;
import org.apache.carbondata.core.scan.result.AbstractScannedSortResult;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.scan.result.ScanResultComparator;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailQuerySortResultIterator extends AbstractDetailQueryResultIterator {

	private final Object lock = new Object();
	Integer executeCnt =0;
	private Future<BatchResult> future;
	//private Integer limitKey = null;
	private SortOrderType sortType = SortOrderType.ASC;
	AbstractScannedSortResult currentScannedResult = null;
	AbstractScannedSortResult nextScannedResult = null;
	
	protected boolean nextBatch = false;
	
	protected TreeSet<AbstractScannedSortResult> scannedResultSet;
	
	AbstractQueryBlockletFilter blockletFilter;

	//protected List<Future<AbstractScannedSortResult>> scanResultfutureList = new ArrayList<Future<AbstractScannedSortResult>>();
	//protected List<Future<AbstractSortScanResult>> nextScanResultfutureList = new ArrayList<Future<AbstractSortScanResult>>();

	public DetailQuerySortResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
			ExecutorService execService)  throws QueryExecutionException{
		super(infos, queryModel, execService);
		
		// according to limit value to reduce the batch size
		resetBatchSizeByLimit(queryModel.getLimit());
		//TODO only consider the single dimension sort
		QueryDimension singleSortDimesion = queryModel.getSortDimensions().get(0);
		sortType = singleSortDimesion.getSortOrder();
		scannedResultSet = new TreeSet<AbstractScannedSortResult>(new ScanResultComparator(sortType));
		try{
		  scanAndGenerateScannedResultSet(queryModel);
		}catch (Exception e){
		  e.printStackTrace();
		  throw new QueryExecutionException("error: " + e.getMessage());
		}
		

		
	}

	private static final LogService LOGGER = LogServiceFactory
			.getLogService(DetailQuerySortResultIterator.class.getName());


	@Override
	public BatchResult next() {
		BatchResult result = null;
		//klong startTime = System.currentTimeMillis();
		try {

			if (future == null) {
				//this.printTreeSet();
				future = execute();
			}
			result = future.get();
			nextBatch = false;
			if (hasNext()) {
				nextBatch = true;
				future = execute();
			} else {
				fileReader.finish();
			}
			//totalScanTime += System.currentTimeMillis() - startTime;
        } catch (Exception ex) {
          try {
            fileReader.finish();
          } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
          }
          throw new RuntimeException(ex);
        }

		return result;
	}

	@Override
	public boolean hasNext() {

		//return true;
		//return hasNextFlg;
		
		if(nextBatch){
			return true;
		}
		boolean nextFlg = false;
		if(limitFlg){
			
			nextFlg = limit > 0 
					&& (scannedResultSet.size() > 0 
							|| (currentScannedResult != null 
								&& currentScannedResult.hasNextForSort()));
		}else {
			
			nextFlg = scannedResultSet.size() > 0
					|| (this.currentScannedResult != null 
						&& this.currentScannedResult.hasNextForSort());
		}
		//boolean nextFlg = (limit > 0  && (scannedResultSet.size() > 0 || this.currentScannedResult.hasNextForSort() ) ) || nextBatch;
		//LOGGER.info("hasNext: "+nextFlg);
		//TODO
//		if(	!nextFlg){
//			
//			LOGGER.info("currentScannedResult getCurrentSortDimentionKey: "+currentScannedResult.getCurrentSortDimentionKey());
//		}
		return nextFlg;
	}

	/**
	 * It scans the block and returns the result with @batchSize
	 *
	 * @return Result of @batchSize
	 */
	public Future<BatchResult> execute() {

		return execService.submit(new Callable<BatchResult>() {
			@Override
			public BatchResult call() throws QueryExecutionException, IOException {

				BatchResult batchResult = new BatchResult();

				List<Object[]> collectedResult = null;
				//long start =System.currentTimeMillis();
				//int tmpCount = 0;
				//boolean tmpflg =updateScanner();
				//LOGGER.info("updateScanner: "+tmpflg);
				if (updateScanner()) {
					
					collectedResult = currentScannedResult.collectSortedData(batchSize, nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);

					decreaseLimit(collectedResult.size());	
					
					//long end =System.currentTimeMillis();
					//LOGGER.info("collectSortedData"+(++tmpCount) +": " +(end -start));
					
					//tmpflg =updateScanner();
					//LOGGER.info("updateScanner: "+tmpflg);
					while (collectedResult.size() < batchSize && limit>0 && updateScanner()) {
						
						  //start =System.currentTimeMillis();
						//List<Object[]> data = currentScannedResult.collectSortedData( batchSize - collectedResult.size(), nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);
						//currentScannedResult.loadOtherColunmsData();
						int leftSize = batchSize - collectedResult.size();
						if(limitFlg &&(limit < leftSize)){
							leftSize = limit;
						}
						List<Object[]> data = currentScannedResult.collectSortedData( leftSize, nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);
						collectedResult.addAll(data);

						decreaseLimit(data.size());
						//printCount(collectedResult);
						
						///tmpflg =updateScanner();
						//LOGGER.info("updateScanner: "+tmpflg);
						 //end =System.currentTimeMillis();
						//LOGGER.info("collectSortedData "+(++tmpCount) +": " +(end -start));
					}
					

				} else {
					collectedResult = new ArrayList<>();
					
				}
				batchResult.setRows(collectedResult);
				//if(collectedResult!=null){
					//LOGGER.info("collectedResult start: " + collectedResult.get(0)[0]);
					//LOGGER.info("collectedResult end: " + collectedResult.get(collectedResult.size()-1)[0]);
				//}
				//LOGGER.info("collectedResult.size(): "+collectedResult.size());
				return batchResult;

			}

			public void printCount(List<Object[]> collectedResult) {
				synchronized (executeCnt){
					LOGGER.info("executeCnt:"+  ++executeCnt);
					//limit = limit -collectedResult.size();
					LOGGER.info("limit left:"+  limit);
					LOGGER.info("collectedResult.size:"+  collectedResult.size());
				}
			}
		});
	}

	protected boolean updateScanner() {
		
		//printTreeSet();
//   		if(currentScannedResult != null && "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//   	   		if( "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//   	   			LOGGER.info("collectedResult.size:");
//   	   		}
//
//		}

		if (currentScannedResult != null) {
			if(currentScannedResult.hasNextForSort()){
				
				//scannedResultSet.remove(currentScannedResult);				
				//printTreeSet();
				if(! currentScannedResult.isCurrentSortDimentionKeyChgFlg()){
				
					return true;
				}
				scannedResultSet.add(currentScannedResult);				
				//printTreeSet();
			}else{
				
				scannedResultSet.remove(currentScannedResult);
/*				if(scannedResultSet.size()<=0){
					//hasNextFlg = false;
					return false;
				}*/
			}
		}
	
		if(scannedResultSet.size()>0){
			currentScannedResult = scannedResultSet.first();
			//printTreeSet();
//	   		if(currentScannedResult != null && "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//	   	   		if( "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//	   	   			LOGGER.info("collectedResult.size:");
//	   	   		}
//
//			}
	   		
			scannedResultSet.remove(currentScannedResult);
			if(scannedResultSet.size()>0){
				nextScannedResult = scannedResultSet.first();
			} else {
				nextScannedResult = null;
			}
		}else{
			//hasNextFlg = false;
			return false;
		}
		

		return true;
	}

	private void printTreeSet() {
		LOGGER.info("start print");
		for(AbstractScannedSortResult detailedScannedResult : this.scannedResultSet){
			
			LOGGER.info("CurrentSortDimentionKey in scannedResultSet: " + detailedScannedResult.getCurrentSortDimentionKey());	
		}
		LOGGER.info("end print");
	}



	private void scanAndGenerateScannedResultSet(QueryModel queryModel)  throws QueryExecutionException, IOException, InterruptedException, ExecutionException, FilterUnsupportedException{

		//LOGGER.info("blockExecutionInfos: "+ blockExecutionInfos.get(0).getNumberOfBlockToScan());
		LimitFilteredBlocksChunkHolders limitFilteredBlocksChunkHolders = new LimitFilteredBlocksChunkHolders(queryModel.getLimit(),
				((BlockExecutionInfo)blockExecutionInfos.get(0)).getAllSortDimensionBlocksIndexes()[0],
				queryModel.getSortDimensions().get(0).getSortOrder());
		for (BlockExecutionInfo executionInfo : (List<BlockExecutionInfo>)blockExecutionInfos) {

			queryStatisticsModel.setRecorder(recorder);
            
            if (executionInfo.getFilterExecuterTree() != null) {
              blockletFilter = new FilterQueryBlockletFilter(executionInfo, fileReader,
                  queryStatisticsModel, execService, sortType);
            } else {
              blockletFilter = new NoFilterQueryBlockletFilter(executionInfo, fileReader,
                  queryStatisticsModel, execService, sortType);
            }
            long start = System.currentTimeMillis();
			blockletFilter.filterDataBlocklets(executionInfo, fileReader, queryStatisticsModel, queryModel, limitFilteredBlocksChunkHolders);
			System.out.println("filterDataBlocklets: "+(System.currentTimeMillis() -start));
		}
		//blockExecutionInfos = null;
		
		long start = System.currentTimeMillis();
		//LOGGER.info("blocksChunkHolderLimitFilter size: "+blocksChunkHolderLimitFilter.getRequiredToScanBlocksChunkHolderSet().size());

		scannedResultSet = generateAllScannedResultSet(execService, limitFilteredBlocksChunkHolders, sortType);
		System.out.println("generateAllScannedResultSet: "+(System.currentTimeMillis() -start));
	}

  // only load the sort dim data ,others will be lazy loaded
  public TreeSet<AbstractScannedSortResult> generateAllScannedResultSet(ExecutorService execService,
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

        scanResultfutureList.add(blockletFilter.executeRead(execService, blocksChunkHolder));

      }

      // long start = System.currentTimeMillis();
      for (int i = 0; i < scanResultfutureList.size(); i++) {
        Future<AbstractScannedSortResult[]> futureResult = scanResultfutureList.get(i);

        AbstractScannedSortResult[] detailedScannedResults = futureResult.get();

        // LOGGER.info("detailedScannedResult.getCurrentSortDimentionKey()"
        // + detailedScannedResult.getCurrentSortDimentionKey());
        if (detailedScannedResults != null) {
          for(AbstractScannedSortResult result : detailedScannedResults){
            scannedResultSet.add(result);
          }
          // printTreeSet(scannedResultSet);
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

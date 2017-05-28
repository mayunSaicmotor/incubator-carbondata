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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.scan.filter.executer.AbstractFilterExecuter.FilterOperator;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used to store bitmap dimension data
 */
public class SafeBitMapDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;
  private byte[][] bitmap_encoded_dictionaries;
  private int[] bitmap_data_pages_offset;
  private BitSetGroup bitSetGroup;
  private boolean isGeneratedBitSetFlg = false;
  private byte[] dictData;

  public SafeBitMapDimensionDataChunkStore(List<Integer> bitmap_encoded_dictionaries,
      List<Integer> bitmap_data_pages_offset, int columnValueSize) {
    super(false);
    this.columnValueSize = columnValueSize;
    int arraySize = bitmap_encoded_dictionaries.size();
    this.bitmap_encoded_dictionaries = new byte[arraySize][];
    this.bitmap_data_pages_offset = new int[bitmap_data_pages_offset.size()];
    for (byte i = 0; i < arraySize; i++) {
      this.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      this.bitmap_data_pages_offset[i] = bitmap_data_pages_offset.get(i);
    }
    this.bitmap_data_pages_offset[arraySize] = bitmap_data_pages_offset.get(arraySize);
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex
   *          inverted index to be stored
   * @param invertedIndexReverse
   *          inverted index reverse to be stored
   * @param data
   *          data to be stored
   */
  @Override
  public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      final byte[] data) {
    this.data = data;
    loadDictionaryDataIndex();
    bitSetGroup = new BitSetGroup(bitmap_encoded_dictionaries.length);
  }

  /**
   * Below method will be used to get the row data
   *
   * @param rowId
   *          Inverted index
   */
  @Override
  public byte[] getRow(int rowId) {
    byte[] data = new byte[1];
    data[0] = dictData[rowId];
    return data;
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param index
   *          row id
   * @return surrogate key
   */
  @Override
  public int getSurrogate(int index) {
    loadAllBitSets();
    return dictData[index];
  }

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId
   *          row id of the data to be filled
   * @param buffer
   *          buffer in which data will be filled
   * @param offset
   *          off the of the buffer
   */
  @Override
  public void fillRow(int rowId, byte[] buffer, int offset) {

    System.arraycopy(dictData[rowId], 0, buffer,
        offset, columnValueSize);
  }

  /**
   * @return size of each column value
   */
  @Override
  public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * to compare the two byte array
   *
   * @param index
   *          index of first byte array
   * @param compareValue
   *          value of to be compared
   * @return compare result
   */
  @Override
  public int compareTo(int index, byte[] compareValue) {
    return ByteUtil.UnsafeComparer.INSTANCE.compareTo(
        this.bitmap_encoded_dictionaries[(int)dictData[index]], 0, columnValueSize,
        compareValue, 0, columnValueSize);
  }

  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
  public BitSet applyFilter(byte[][] filterValues, FilterOperator operator, int numerOfRows) {

    byte[] inBitSet = new byte[bitmap_encoded_dictionaries.length];
    byte inCnt = 0;
    // BitSet notInBitSet = new BitSet(bitmap_encoded_dictionaries.length);
    for (byte i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
          bitmap_encoded_dictionaries[i]);
      if (index >= 0) {
        inBitSet[i]=1;
        inCnt++;
        // inBitSetList.add(bitSetGroup.getBitSet(i));
      }
    }
    if (FilterOperator.NOT_IN.equals(operator)) {
      return getBitSetResult(numerOfRows, inBitSet, true, inCnt);
    } else {
      return getBitSetResult(numerOfRows, inBitSet, false, inCnt);
    }
  }

  private BitSet getBitSetResult(int numerOfRows, byte[] inBitSetList, boolean notInFlg, byte inCnt) {

    // int inCnt = inBitSetList.cardinality();
    if ((notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (!notInFlg && inCnt == 0)) {
      return null;
    }
    if ((!notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (notInFlg && inCnt == 0)) {
      if (bitmap_encoded_dictionaries.length == 1) {
        return loadBitSet(0);
      }
      BitSet resultBitSet = new BitSet(numerOfRows);
      resultBitSet.flip(0, numerOfRows);
      return resultBitSet;
    }

    if (inCnt << 2 < bitmap_encoded_dictionaries.length) {
      return bitSetOr(inBitSetList, notInFlg, numerOfRows, (byte)1);
    } else {
      //inBitSetList.flip(0, bitmap_encoded_dictionaries.length);
      return bitSetOr(inBitSetList, !notInFlg, numerOfRows, (byte)0);
    }
  }

  private BitSet bitSetOr(byte[] bitSetList, boolean flipFlg, int numerOfRows, byte equalValue) {
    BitSet resultBitSet = null;
    for(byte i =0; i< bitSetList.length; i++){
      if(bitSetList[i] == equalValue){
        if (resultBitSet == null) {
          resultBitSet = loadBitSet(i);
        } else {
          resultBitSet.or(loadBitSet(i));
        }
      }
    }
    if (flipFlg) {
      resultBitSet.flip(0, numerOfRows);
    }
    return resultBitSet;
  }

  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
   public BitSet applyFilter2(byte[][] filterValues, FilterOperator operator, int numerOfRows) {
    BitSet bitSet = null;
    for (byte i = 0; i < filterValues.length; i++) {
      int index = CarbonUtil.binarySearch(bitmap_encoded_dictionaries, 0,
          bitmap_encoded_dictionaries.length - 1, filterValues[i]);
      if (index >= 0) {
        if (bitSet == null) {
          bitSet = loadBitSet(index);
        } else {
          bitSet.or(loadBitSet(index));
        }
      }
    }
    isGeneratedBitSetFlg = true;
    if (FilterOperator.NOT_IN.equals(operator)) {
      if (bitSet == null) {
        bitSet = new BitSet(numerOfRows);
      }
      bitSet.flip(0, numerOfRows);
    }
    return bitSet;
  }
  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
  public BitSet applyFilter1(byte[][] filterValues, FilterOperator operator, int numerOfRows) {

    BitSet inBitSet = new BitSet(bitmap_encoded_dictionaries.length);
    // BitSet notInBitSet = new BitSet(bitmap_encoded_dictionaries.length);
    for (byte i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
          bitmap_encoded_dictionaries[i]);
      if (index >= 0) {
        inBitSet.set(i, true);
        // inBitSetList.add(bitSetGroup.getBitSet(i));
      }
    }
    if (FilterOperator.NOT_IN.equals(operator)) {
      return getBitSetResult(numerOfRows, inBitSet, true);
    } else {
      return getBitSetResult(numerOfRows, inBitSet, false);
    }
  }

  private BitSet getBitSetResult(int numerOfRows, BitSet inBitSetList, boolean notInFlg) {

    int inCnt = inBitSetList.cardinality();
    if ((notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (!notInFlg && inCnt == 0)) {
      return null;
    }
    if ((!notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (notInFlg && inCnt == 0)) {
      if (bitmap_encoded_dictionaries.length == 1) {
        return loadBitSet(0);
      }
      BitSet resultBitSet = new BitSet(numerOfRows);
      resultBitSet.flip(0, numerOfRows);
      return resultBitSet;
    }

    if (inCnt << 2 < bitmap_encoded_dictionaries.length) {
      return bitSetOr(inBitSetList, notInFlg, numerOfRows);
    } else {
      inBitSetList.flip(0, bitmap_encoded_dictionaries.length);
      return bitSetOr(inBitSetList, !notInFlg, numerOfRows);
    }
  }

  private BitSet bitSetOr(BitSet bitSetList, boolean flipFlg, int numerOfRows) {
    BitSet resultBitSet = null;
    for (int i = bitSetList.nextSetBit(0); i >= 0; i = bitSetList.nextSetBit(i + 1)) {
      if (resultBitSet == null) {
        resultBitSet = loadBitSet(i);
      } else {
        resultBitSet.or(loadBitSet(i));
      }
    }
    if (flipFlg) {
      resultBitSet.flip(0, numerOfRows);
    }
    return resultBitSet;
  }
  private void loadAllBitSets() {
    if (isGeneratedBitSetFlg) {
      return;
    }
    for (byte i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      bitSetGroup.setBitSet(loadBitSet(i), i);
    }
    isGeneratedBitSetFlg = true;
  }

  private BitSet loadBitSet(int index) {
    BitSet bitSet = this.bitSetGroup.getBitSet(index);
    if (bitSet != null) {
      return bitSet;
    }
    int tempIndex = index + 1;
    int pageOffSet = bitmap_data_pages_offset[tempIndex];
    int pageLength;
    if (tempIndex + 1 == bitmap_data_pages_offset.length) {
      pageLength = data.length - pageOffSet;
    } else {
      pageLength = bitmap_data_pages_offset[tempIndex + 1] - bitmap_data_pages_offset[tempIndex];
    }
    byte[] bitSetData = new byte[pageLength];
    System.arraycopy(this.data, pageOffSet, bitSetData, 0, pageLength);
    bitSet = BitSet.valueOf(bitSetData);
    bitSetGroup.setBitSet(bitSet, index);
    return bitSet;
  }

  private void loadDictionaryDataIndex() {
    int pageOffSet = 0;
    int pageLength = bitmap_data_pages_offset[1];
    dictData = new byte[pageLength];
    System.arraycopy(this.data, pageOffSet, dictData, 0, pageLength);
  }
}

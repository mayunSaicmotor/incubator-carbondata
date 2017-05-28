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
package org.apache.carbondata.core.datastore.columnar;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to for bitmap encoded column
 */
public class BlockIndexerStorageForBitMapForShort implements IndexStorage<short[]> {

  /**
   * column data
   */
  private byte[][] keyBlock;

  /**
   * total number of rows
   */
  private int totalSize;

  private byte[] min;
  private byte[] max;
  private Map<Integer, BitSet> dictMap;
  List<Integer> dictList;
  List<Integer> bitMapPagesOffsetList;
  private short[] dataIndexMap;
  public BlockIndexerStorageForBitMapForShort(byte[][] keyBlockInput, boolean isNoDictionary) {
    generateBitmapData(keyBlockInput);
    compressDataMyOwnWay(keyBlock[0]);
  }

  private void generateBitmapData(byte[][] keyBlockInput) {
    dictMap = new TreeMap<Integer, BitSet>();
    min = keyBlockInput[0];
    max = keyBlockInput[0];
    int minCompare = 0;
    int maxCompare = 0;
    BitSet dictBitSet = null;
    int sizeInBitSet = keyBlockInput.length % 8 > 0 ? (keyBlockInput.length / 8 + 1) * 8
        : keyBlockInput.length;
    // generate dictionary data page
    byte[] dictionaryDataPage = new byte[keyBlockInput.length];
    for (int i = 0; i < keyBlockInput.length; i++) {
      dictionaryDataPage[i] = keyBlockInput[i][0];
      int dictKey = dictionaryDataPage[i];
      dictBitSet = dictMap.get(dictKey);
      if (dictBitSet == null) {
        dictBitSet = new BitSet(sizeInBitSet);
        dictMap.put(dictKey, dictBitSet);
      }
      dictBitSet.set(i, true);
      minCompare = ByteUtil.compare(min, keyBlockInput[i]);
      maxCompare = ByteUtil.compare(max, keyBlockInput[i]);
      if (minCompare > 0) {
        min = keyBlockInput[i];
      }
      if (maxCompare < 0) {
        max = keyBlockInput[i];
      }
    }
    dictList = new ArrayList<Integer>(dictMap.size());
    bitMapPagesOffsetList = new ArrayList<Integer>(dictMap.size());
    keyBlock = new byte[dictMap.size() + 1][];
    keyBlock[0] = dictionaryDataPage;
    bitMapPagesOffsetList.add(totalSize);
    totalSize = dictionaryDataPage.length;
    int index = 1;
    for (Integer dictKey : dictMap.keySet()) {
      dictBitSet = dictMap.get(dictKey);
      int byteArrayLength = dictBitSet.toByteArray().length;
      bitMapPagesOffsetList.add(totalSize);
      totalSize = totalSize + byteArrayLength;
      dictList.add(dictKey);
      keyBlock[index++] = dictBitSet.toByteArray();
    }
  }

  @Override
  public short[] getDataIndexMap() {
    return dataIndexMap;
  }

  @Override
  public int getTotalSize() {
    return totalSize;
  }

  @Override
  public boolean isAlreadySorted() {
    return false;
  }

  /**
   * no use
   *
   * @return
   */
  @Override
  public short[] getDataAfterComp() {
    return new short[0];
  }

  /**
   * no use
   *
   * @return
   */
  @Override
  public short[] getIndexMap() {
    return new short[0];
  }

  /**
   * @return the keyBlock
   */
  public byte[][] getKeyBlock() {
    return keyBlock;
  }

  @Override
  public byte[] getMin() {
    return min;
  }

  @Override
  public byte[] getMax() {
    return max;
  }

  public List<Integer> getDictList() {
    return dictList;
  }

  public void setDictList(List<Integer> dictList) {
    this.dictList = dictList;
  }

  public List<Integer> getBitMapPagesOffsetList() {
    return bitMapPagesOffsetList;
  }

  public void setBitMapPagesOffsetList(List<Integer> bitMapPagesLengthList) {
    this.bitMapPagesOffsetList = bitMapPagesLengthList;
  }

  /**
   * Create an object with each column array and respective index
   *
   * @return
   */
  private ColumnWithIntIndex[] createColumnWithIndexArray() {
    ColumnWithIntIndex[] columnWithIndexs;
    columnWithIndexs = new ColumnWithIntIndex[this.totalSize];
    int cnt = 0;
    for (short i = 0; i < 1; i++) {
      for (short j = 0; j < keyBlock[i].length; j++) {
        byte[] value = new byte[1];
        value[0] = keyBlock[i][j];
        columnWithIndexs[cnt] = new ColumnWithIntIndex(value, cnt);
        cnt++;
      }
      
    }

    return columnWithIndexs;
  }

  private void compressDataMyOwnWay(byte[] indexes) {
    byte[] prvKey = new byte[1];
    prvKey[0]=indexes[0];
    List<Byte> list = new ArrayList<Byte>(indexes.length / 2);
    list.add(indexes[0]);
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < indexes.length; i++) {
      byte[] indexData = new byte[1];
      indexData[0] = indexes[i];
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, indexData) != 0) {
        //prvKey = indexes[i].getColumn();
        prvKey = new byte[1];
        prvKey[0]=indexes[i];
        list.add(indexes[i]);
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / indexes.length) < 70;
    if (useRle) {
      this.keyBlock = convertToKeyArray(list);
      dataIndexMap = convertToArray(map);
    } else {
      // this.keyBlock = convertToKeyArray(indexes);
      dataIndexMap = new short[0];
    }
    // generateBitMapPagesOffsets();
  }

  private short[] convertToArray(List<Short> list) {
    short[] shortArray = new short[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }
/*  private byte[][] convertToKeyArray(byte[] indexes) {
    byte[][] shortArray = new byte[indexes.length][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i][0] = indexes[i];
      totalSize += shortArray[i].length;
    }
    return shortArray;
  }*/

  private byte[][] convertToKeyArray(List<Byte> list) {
    byte[] shortArray = new byte[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
      // totalSize += shortArray[i].length;
    }
    totalSize = totalSize + shortArray.length - keyBlock[0].length;
    keyBlock[0] = shortArray; 
    return keyBlock;
  }

//  private void generateBitMapPagesOffsets() {
//    bitMapPagesOffsetList = new ArrayList<Integer>(dictMap.size());
//    for (int i = 0; i < keyBlock.length; i++) {
//      bitMapPagesOffsetList.add(totalSize);
//      totalSize += keyBlock[i].length;
//    }
//  }
}

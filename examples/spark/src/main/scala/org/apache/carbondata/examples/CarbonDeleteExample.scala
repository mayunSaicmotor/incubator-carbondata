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

package org.apache.carbondata.examples

import scala.collection.mutable.LinkedHashMap

import org.apache.carbondata.examples.util.ExampleUtils

object CarbonDeleteExample {
  def main(args: Array[String]) {

    CarbonDeleteExample.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {

    val spark = ExampleUtils.createCarbonContext("CarbonDeleteExample")
    spark.sparkContext.setLogLevel("WARN")

    // scalastyle:off println
    var maxTestTimes = 30
    var timeCostSeq = Seq[LinkedHashMap[String, Long]]()
    var start: Long = System.currentTimeMillis()
    var startTime: Long = System.currentTimeMillis()
    var testName: String = null
    var intervalCnt = 1;

    if (args != null && args.length > 0) {
      intervalCnt = args.last.toInt
    }

    spark.sql(s"""
             delete from $tableName
             WHERE name =  "name1600006"
             """).show()
    spark.sql(s"""
             delete from $tableName
             WHERE name = "name1600001"
             """).show()
      spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             where id between 4000001 and 4000010
             order by ID
             """).show(10000)
  }
}

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

object CarbonLoopDeleteExample {
  def main(args: Array[String]) {

    CarbonLoopDeleteExample.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {

    val spark = ExampleUtils.createCarbonContext("CarbonLoopDeleteExample")
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             where id between 4000001 and 4000031
             order by ID
             """).show(10000)

    spark.sql(s"""
             SELECT count(*)
             FROM $tableName
             """).show()

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
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      testName = "name" + (1600000 + testNo * intervalCnt)
      println("testName: " + testName)
      spark.sql(s"""
       select * from $tableName
       WHERE name = '$testName'
       """).show()
      startTime = System.currentTimeMillis()
      spark.sql(s"""
             delete from $tableName
             WHERE name = '$testName'
             """).show()
      timeCostMap += ("single delete time: "
        -> new java.lang.Long(System.currentTimeMillis() - startTime))
      println("single delete time: " + (System.currentTimeMillis() - startTime))

      startTime = System.currentTimeMillis()
      spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             WHERE name = '$testName'
             """).show(100)
      timeCostMap += ("select deleted data time "
        -> new java.lang.Long(System.currentTimeMillis() - startTime))
      println("select deleted data time: " + (System.currentTimeMillis() - startTime))

      timeCostSeq = timeCostSeq :+ timeCostMap
      spark.sql(s"""
             SELECT count(*)
             FROM $tableName
             """).show()
    }
    println("delete time: " + (System.currentTimeMillis() - start))
    spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             where id between 4000001 and 4000031
             order by ID
             """).show(10000)
    spark.sql(s"""
             SELECT count(*)
             FROM $tableName
             """).show()
    // use to get statistical information
    for (timeCostMap <- timeCostSeq) {
      for (timeCost <- timeCostMap) {
        print(timeCost._2 + "\t")
      }
      println()
    }
  }
}

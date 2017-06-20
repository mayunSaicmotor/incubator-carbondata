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

import java.io.File

import scala.collection.mutable.LinkedHashMap

import org.apache.spark.sql.SparkSession

object CarbonLoopFirstDeleteThenInsertExample {
  def main(args: Array[String]) {

    CarbonLoopFirstDeleteThenInsertExample.extracted("t3", args)
  }
  def extracted(tableName: String, args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
      + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"
    val testData = s"$rootPath/examples/spark2/src/main/resources/bitmaptest2.csv"
    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonDataLoad")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")
    spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             where id between 4000001 and 4000031
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
    var intervalCnt = 1;

    if (args != null && args.length > 0) {
      intervalCnt = args.last.toInt
    }
    for (testNo <- 1 to maxTestTimes) {
      var id = 4000000 + testNo;
      var date = "2015/8/20"
      var testName = "name" + (1600000 + testNo * intervalCnt)
      var phoneType = "phone" + testNo
      var serialname = "serialname" + testNo
      var salary = 500000 + testNo
      var country = "china" + testNo
      var timeCostMap = LinkedHashMap[String, Long]();

      println("testName: " + testName)
      startTime = System.currentTimeMillis()
      spark.sql(s"""
             delete from $tableName
             WHERE name = '$testName'
             """).show()
      timeCostMap += ("single update time: "
        -> new java.lang.Long(System.currentTimeMillis() - startTime))
      println("single delete time: " + (System.currentTimeMillis() - startTime))

      startTime = System.currentTimeMillis()
      spark.sql(s"""
             insert into $tableName select $id,'$date','$country','$testName'
             ,'$phoneType','$serialname',$salary
             """).show()
      timeCostMap += ("select insert data time "
        -> new java.lang.Long(System.currentTimeMillis() - startTime))
      println("select insert data time: " + (System.currentTimeMillis() - startTime))

      timeCostSeq = timeCostSeq :+ timeCostMap
    }
    println("delete time: " + (System.currentTimeMillis() - start))
    spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country
             FROM $tableName
             where id between 4000001 and 4000031
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

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

object CarbonDeleteExample {
  def main(args: Array[String]) {

    CarbonDeleteExample.extracted("t3")
  }
  def extracted(tableName: String): Unit = {
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
    // scalastyle:off println
    var maxTestTimes = 1
    var timeCostSeq = Seq[LinkedHashMap[String, Long]]()
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      var start: Long = System.currentTimeMillis()

      spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country2
             FROM $tableName
             WHERE country2 = 'france'
             """).show(10000)

      timeCostMap += ("country2 = 'france': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'france': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      spark.sql(s"""
             delete from $tableName
             WHERE country2 = 'france'
             """).show()

      timeCostMap += ("update country2 = 'france': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("delete country2 = 'france': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      spark.sql(s"""
             SELECT ID,date,name,phonetype,serialname,salary,country2
             FROM $tableName
             WHERE name = 'carbon_france'
             """).show(10000)

      timeCostMap += ("country2 = 'france': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'france': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      spark.sql(s"""
             SELECT count(*)
             FROM $tableName
             WHERE country2 = 'china'
             """).show()

      timeCostMap += ("count country2 = 'china': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("count country2 = 'china': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      spark.sql(s"""
             update $tableName set (name, phonetype) = ('carbon_china', 'phonetype_china')
             WHERE country2 = 'china'
             """).show()

      timeCostMap += ("update country2 = 'china': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("update country2 = 'china': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      spark.sql(s"""
             SELECT count(*)
             FROM $tableName
             WHERE name = 'carbon_china'
             """).show(10000)

      timeCostMap += ("name = 'carbon_china': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("name = 'carbon_china': " + (System.currentTimeMillis() - start))

      timeCostSeq = timeCostSeq :+ timeCostMap
    }
    // Drop table
    // spark.sql("DROP TABLE IF EXISTS $tableName")

    // use to get statistical information
    for (timeCostMap <- timeCostSeq) {
      for (timeCost <- timeCostMap) {
        print(timeCost._2 + "    ")
      }
      println()
    }
  }
}

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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSelectBitMapColumnLikeFilterQuery {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonBitMapFilterQueryExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    /*    cc.sql("DROP TABLE IF EXISTS t3")

    // Create table, 6 dimensions, 2 measure
    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Date, country2 String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           TBLPROPERTIES ('BITMAP'='country2')
           """)
    // Load data
    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)
*/
    cc.sql("""
     SELECT count(*)
     FROM t3
     """).show(10)
    cc.sql("""
           SELECT country0, count(*)
           FROM t3
           group by country0
           """).show(10)
    cc.sql("""
           SELECT country1, count(*)
           FROM t3
           group by country1
           """).show(10)
    cc.sql("""
           SELECT country2, count(*)
           FROM t3
           group by country2
           """).show(10)
//    cc.sql("""
//           SELECT count(*)
//           FROM t3
//           """).show(10)

    // scalastyle:off println
    var maxTestTimes = 5
    var timeCostSeq =Seq[LinkedHashMap[String, Long]]()
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      var start = System.currentTimeMillis()
      cc.sql("""
           SELECT country2, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 <> 'korea'
           order by country2
           """).show(10)

      timeCostMap += ("country2 <> 'korea': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 <> 'korea': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql("""
           SELECT country2, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 = 'korea' or country2 = 'poland'
           order by country2
           """).show(10)

      timeCostMap += ("country2 = 'korea': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'korea': " + (System.currentTimeMillis() - start))

      cc.sql("""
           SELECT country2, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 like 'c%'
           """).show(10)

      timeCostMap += ("country2 like 'c%' query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 like 'c%' query time: "
        + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql("""
           SELECT country2, serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 like 'f%'
           order by country2
           """).show(10)

      timeCostMap += ("country2 like 'u%' order query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 like 'u%' count query time: "
        + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()

      cc.sql("""
           SELECT count(country0)
           FROM t3
           WHERE country0 like 'u%'
           """).show(10)

      timeCostMap += ("country0 like 'u%' count query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country0 like 'u%' count query time: "
        + (System.currentTimeMillis() - start))

      cc.sql("""
           SELECT count(country1)
           FROM t3
           WHERE country1 like 'u%'
           """).show(10)

      timeCostMap += ("country1 like 'u%' count query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country1 like 'u%' count query time: "
        + (System.currentTimeMillis() - start))
      timeCostSeq = timeCostSeq :+ timeCostMap
      cc.sql("""
           SELECT count(country2)
           FROM t3
           WHERE country2 like 'u%'
           """).show(10)

      timeCostMap += ("country2 like 'u%' count query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 like 'u%' count query time: "
        + (System.currentTimeMillis() - start))
    }
    // Drop table
    // cc.sql("DROP TABLE IF EXISTS t3")

    // use to get statistical information
    for (timeCostMap <- timeCostSeq) {
      for (timeCost <- timeCostMap) {
        print(timeCost._2 + "	 ")
      }
      println()
    }
    // scalastyle:on println
  }
}

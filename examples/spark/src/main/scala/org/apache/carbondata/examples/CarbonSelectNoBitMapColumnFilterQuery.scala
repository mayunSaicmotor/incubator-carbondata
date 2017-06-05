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

object CarbonSelectNoBitMapColumnFilterQuery {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonBitMapFilterQueryExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    // scalastyle:off println
    var maxTestTimes = 4
    var timeCostSeq = Seq[LinkedHashMap[String, Long]]()
    for (testNo <- 1 to maxTestTimes) {
      var timeCostMap = LinkedHashMap[String, Long]();
      var start: Long = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 <> 'china'
           
           """).show(10)
      }
      timeCostMap += ("country2 <> 'china': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 <> 'china': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 = 'china'
           
           """).show(10)
      }
      timeCostMap += ("country2 = 'china': "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'china': " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 <> 'france'
           
           """).show(10)
      }
      timeCostMap += ("country2 <> 'france' query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 <> 'france' query time: " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 = 'france'
           
           """).show(10)
      }
      timeCostMap += ("country2 = 'france' query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'france' query time: " + (System.currentTimeMillis() - start))
      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 IN ('france')
           
           """).show(10)
      }
      timeCostMap += ("country2 IN ('france') query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 IN ('france') query time: " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 <> 'china' and country2 <> 'canada' and country2 <> 'indian'
           and country2 <> 'uk'
           
           """).show(10)
      }
      timeCostMap += ("country2 <> 'china' and country2 <> 'canada' and country2 <> 'indian'"
        + "and country2 <> 'uk' query time query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 <> 'china' and country2 <> 'canada' and country2 <> 'indian'"
        + "and country2 <> 'uk' query time query time: "
        + (System.currentTimeMillis() - start))
      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 not in ('china','canada','indian','usa','uk')
           
           """).show(10)
      }
      timeCostMap += ("country2 not in ('china','canada','indian','usa','uk') query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 not in ('china','canada','indian','usa','uk') query time: "
        + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 IN ('china','usa','uk')
           
           """).show(10)
      }
      timeCostMap += ("country2 IN ('china','usa','uk') query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 IN ('china','usa','uk') query time: " + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 = 'china' or country2 = 'indian' or country2 = 'usa'
           
           """).show(10)
      }
      timeCostMap += ("country2 = 'china' or country2 = 'indian' or country2 = 'usa' query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 = 'china' or country2 = 'indian' or country2 = 'usa' query time: "
        + (System.currentTimeMillis() - start))

      start = System.currentTimeMillis()
      for (index <- 1 to 1) {
        cc.sql("""
           SELECT serialname, phonetype, salary, name, id
           FROM t3
           WHERE country2 between 'china' and 'indian'
           
           """).show(10)
      }
      timeCostMap += ("country2 between 'china' and 'indian' query time: "
        -> new java.lang.Long(System.currentTimeMillis() - start))
      println("country2 between 'china' and 'indian' query time: "
        + (System.currentTimeMillis() - start))

      timeCostSeq = timeCostSeq :+ timeCostMap
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

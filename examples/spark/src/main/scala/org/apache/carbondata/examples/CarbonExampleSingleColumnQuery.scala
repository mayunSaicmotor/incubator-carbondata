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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonExampleSingleColumnQuery {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    for (index <- 1 to 1) {
    var start = System.currentTimeMillis()

      cc.sql("""
           SELECT name FROM t3 ORDER BY name desc limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT name FROM t3 ORDER BY name  limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT name, salary FROM t3 ORDER BY name desc limit 10002
           """).show(1000000)
      cc.sql("""
           SELECT name, salary FROM t3 ORDER BY name  limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT salary FROM t3 ORDER BY name desc limit 10002
           """).show(1000000)
      cc.sql("""
           SELECT salary FROM t3 ORDER BY name  limit 10002
           """).show(1000000)

      var end = System.currentTimeMillis()
      print("query time: " + (end - start))

      start = System.currentTimeMillis()

      cc.sql("""
           SELECT date, salary FROM t3 ORDER BY date desc limit 10002
           """).show(1000000)
      cc.sql("""
           SELECT date, salary FROM t3 ORDER BY date  limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT salary FROM t3 ORDER BY date desc limit 10002
           """).show(1000000)
      cc.sql("""
           SELECT salary FROM t3 ORDER BY date  limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT date FROM t3 ORDER BY date desc limit 1002
           """).show(1000000)

      cc.sql("""
           SELECT date FROM t3 ORDER BY date limit 1002
           """).show(1000000)

      end = System.currentTimeMillis()
      print("query time: " + (end - start))

      start = System.currentTimeMillis()

            cc.sql("""
           SELECT name, salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name desc limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT name, salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name  limit 10002
           """).show(1000000)
           
      cc.sql("""
           SELECT salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name desc limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name  limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT name FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name desc limit 10002
           """).show(1000000)

      cc.sql("""
           SELECT name FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY name  limit 10002
           """).show(1000000)

      end = System.currentTimeMillis()
      print("query time: " + (end - start))

      start = System.currentTimeMillis()
      
            cc.sql("""
           SELECT date, salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date desc limit 1002
           """).show(1000000)

      cc.sql("""
           SELECT date, salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date limit 1002
           """).show(1000000)
           
      cc.sql("""
           SELECT salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date desc limit 1002
           """).show(1000000)

      cc.sql("""
           SELECT salary FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date limit 1002
           """).show(1000000)

      cc.sql("""
           SELECT date FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date desc limit 1002
           """).show(1000000)

      cc.sql("""
           SELECT date FROM t3 WHERE country IN ('uk','usa','canada') ORDER BY date limit 1002
           """).show(1000000)

      end = System.currentTimeMillis()
      print("query time: " + (end - start))



    }
  }
}

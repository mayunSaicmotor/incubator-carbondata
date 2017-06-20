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

import org.apache.carbondata.examples.util.ExampleUtils

object CarbonDataLoad {

  def main(args: Array[String]) {
    val testData = ExampleUtils.currentPath + "/src/main/resources/uiddata.csv"
    var spark = ExampleUtils.createCarbonContext("CarbonDataLoad")
    // hash partition
    spark.sql("DROP TABLE IF EXISTS t3")
    spark.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Date, country String,
           name String, phonetype String, serialname char(10), salary Int)
           STORED BY 'carbondata'
           """)

    // Load data
    spark.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)
    //    spark.sql(s"""
    //           LOAD DATA LOCAL INPATH '$testData' into table t3
    //           """)
    //    spark.sql(s"""
    //           LOAD DATA LOCAL INPATH '$testData' into table t3
    //           """)
    //    spark.sql(s"""
    //           LOAD DATA LOCAL INPATH '$testData' into table t3
    //           """)
    spark.sql("""
     SELECT count(*)
     FROM t3
     """).show(10)
  }

}

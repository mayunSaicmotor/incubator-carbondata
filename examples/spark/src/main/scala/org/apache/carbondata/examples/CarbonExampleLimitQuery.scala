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

object CarbonExampleLimitQuery {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/data.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/MM/dd")

    //cc.sql("DROP TABLE IF EXISTS t3")
    //cc.sql("use default")

    /*    cc.sql("""
           CREATE TABLE IF NOT EXISTS t3
           (ID Int, date Timestamp, country String,
           name String, phonetype String, serialname String, salary Int)
           STORED BY 'carbondata'
           """)

    cc.sql(s"""
           LOAD DATA LOCAL INPATH '$testData' into table t3
           """)*/

    //cc.sql("desc t3").show();

    /*    cc.sql("""
     SELECT country, name
     FROM t3
     WHERE country IN ('uk','usa','canada')
     """).show(200)
           
    cc.sql("""
           SELECT country
           FROM t3
           WHERE country IN ('uk','usa','canada')
           GROUP BY country
     
           """).show(1)

         cc.sql("""
           SELECT country
           FROM t3
            limit 1
     
           """).show(1)*/

    for (index <- 1 to 1) {


      var start = System.currentTimeMillis()

            cc.sql("""
           SELECT name,name1,name2,name3,name4,name5,name6,name7,name8, date, serialname,country,salary FROM t3 ORDER BY date limit 1500
           """).show(1000000)


      var end = System.currentTimeMillis()
      print("query time: " + (end - start))

      start = System.currentTimeMillis()
    cc.sql("""
           SELECT name, serialname, country, salary, id, date FROM t3 ORDER BY country limit 2000
           """).show(1000000)
           
    end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 
   
   
         start = System.currentTimeMillis()
    cc.sql("""
           SELECT name, serialname, country, salary, id, date FROM t3 ORDER BY serialname limit 10000
           """).show(1000000)
           
    end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 
   
         start = System.currentTimeMillis()
    cc.sql("""
           SELECT name, serialname, country, salary, id, date FROM t3 ORDER BY serialname, country limit 10000
           """).show(1000000)
           
    end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 

      /*         start = System.currentTimeMillis()
    cc.sql("""
           SELECT name, serialname, country, salary, id, date FROM t3 ORDER BY name desc limit 500
           """).show(1000000)
           
    end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 
   
   start = System.currentTimeMillis()
    cc.sql("""
           SELECT date, serialname, country, salary FROM t3 ORDER BY date  desc limit 1002
           """).show(1000000)
           
   end = System.currentTimeMillis() 
   print("query time: " + (end- start)) */

      /* start = System.currentTimeMillis()
    cc.sql("""
           SELECT serialname, country, phonetype, salary, name, id FROM t3 ORDER BY serialname limit 100
           """).show(20000)
           
   end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 
   
      start = System.currentTimeMillis()
    cc.sql("""
           SELECT serialname, country, phonetype, salary, name, id FROM t3 ORDER BY serialname limit 1000
           """).show(20000)
           
   end = System.currentTimeMillis() 
   print("query time: " + (end- start)) */

      /*  start = System.currentTimeMillis()
    cc.sql("""
           SELECT serialname, country, phonetype, salary, name, id FROM t3 ORDER BY serialname limit 10000
           """).show(10000)
           
   end = System.currentTimeMillis() 
   print("query time: " + (end- start)) 
   */
      /*
   
     start = System.currentTimeMillis()
    cc.sql("""
           SELECT country, name, phonetype, salary, id FROM t3 limit 10
           """).show(100000)
           
    end = System.currentTimeMillis() 
   print("query time: " + (end- start))    
 start = System.currentTimeMillis() 
   
        cc.sql("""
           SELECT country, serialname, phonetype
           FROM t3
           
           ORDER BY serialname
           """).show(1000)
 
   end = System.currentTimeMillis() 
   print("query time: " + (end- start))   */
    }
    /*  start = System.currentTimeMillis() 
    
    cc.sql("""
           SELECT country, serialname, phonetype
           FROM t3
           
           ORDER BY serialname
           """).show(20)
   end = System.currentTimeMillis() 
   print("query time: " + (end- start))    
   start = System.currentTimeMillis() 
    
    cc.sql("""
           SELECT name, count(country), sum(salary)
           FROM t3
           
           GROUP BY name  
           """).show(20)
   end = System.currentTimeMillis() 
   print("query time: " + (end- start))    
   start = System.currentTimeMillis() 
    //cc.sql("DROP TABLE IF EXISTS t3")
*/ }
}

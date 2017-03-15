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

package org.apache.carbondata.examples.saic

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils
import java.util.Date

object SaicFilterQuery {
  def main(args: Array[String]) {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")
    val testData = ExampleUtils.currentPath + "/src/main/resources/rx5_parquet_10m.csv"

    // Specify timestamp format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd hh:mm:ss")

      
       cc.sql("""
          select 
           *
          from rx5_tbox_parquet_all 
          order by vin 
          limit 10000
           """).show(10)
  
    var start = System.currentTimeMillis()
                       
          
      
        cc.sql("""
          select 
           vin,
            gnsstime,
            vehsyspwrmod,
            vehdoorfrontpas,
            vehdoorfrontdrv,
            vehdoorrearleft,
            vehdoorrearright,
            vehbonnet,
            vehboot
          from rx5_tbox_parquet_all 
           where  vin in ('LSJA24U62HS017126', 'LSJA24U60GG141090', 'LSJA24U60GG130493', 'LSJA24U63GS048898')
          order by vin 
          limit 20000
           """).show(500000)
      
      
      
        
               var end = System.currentTimeMillis()
      print("limit 20000 query time: " + (end - start))
      
      start = System.currentTimeMillis()
           
           
        cc.sql("""
          select 
           vin,
            gnsstime,
            vehsyspwrmod,
            vehdoorfrontpas,
            vehdoorfrontdrv,
            vehdoorrearleft,
            vehdoorrearright,
            vehbonnet,
            vehboot
          from rx5_tbox_parquet_all 
           where  vin in ('LSJA24U62HS017126', 'LSJA24U60GG141090', 'LSJA24U60GG130493', 'LSJA24U63GS048898')
          order by vin 
          limit 2000
           """).show(500000)
           
        
                  end = System.currentTimeMillis()
      print(" limit 2000 query time: " + (end - start))
      
      start = System.currentTimeMillis()
      
                   cc.sql("""
          select 
           vin,
            gnsstime,
            vehsyspwrmod,
            vehdoorfrontpas,
            vehdoorfrontdrv,
            vehdoorrearleft,
            vehdoorrearright,
            vehbonnet,
            vehboot,
            vehwindowfrontleft,
            vehwindowrearleft,
            vehwindowfrontright,
            vehwindowrearright
          from rx5_tbox_parquet_all 
                    where  vin in ('LSJA24U62HS017126', 'LSJA24U60GG141090', 'LSJA24U60GG130493', 'LSJA24U63GS048898')
          order by vin 
          limit 200
           """).show(500000)

       end = System.currentTimeMillis()
      print("limit 200 query time: " + (end - start))
      

/*    for (index <- 1 to 1) {
      var start = System.currentTimeMillis()
      cc.sql("""
          select 
            vin,
            gnsstime,
            vehsyspwrmod,
            vehdoorfrontpas,
            vehdoorfrontdrv,
            vehdoorrearleft,
            vehdoorrearright,
            vehbonnet,
            vehboot,
            vehwindowfrontleft,
            vehwindowrearleft,
            vehwindowfrontright,
            vehwindowrearright,
            vehsunroof,
            vehcruiseactive,
            vehcruiseenabled,
            vehseatbeltdrv 
          from rx5_tbox_parquet_all 
          order by vin 
          limit 100002
           """).show(1000000)

      var end = System.currentTimeMillis()
      print("query time: " + (end - start))

      start = System.currentTimeMillis()
      cc.sql("""
          select 
            gnsstime,
            vin,
            vehsyspwrmod,
            vehdoorfrontpas,
            vehdoorfrontdrv,
            vehdoorrearleft,
            vehdoorrearright,
            vehbonnet,
            vehboot,
            vehwindowfrontleft,
            vehwindowrearleft,
            vehwindowfrontright,
            vehwindowrearright,
            vehsunroof,
            vehcruiseactive,
            vehcruiseenabled,
            vehseatbeltdrv 
          from rx5_tbox_parquet_all 
          order by gnsstime 
          limit 100002
           """).show(1000000)

      end = System.currentTimeMillis()
      print("query time: " + (end - start))
    }*/
  }
}

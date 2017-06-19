package test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class CreateTestData {

  public CreateTestData() {

  }

  public static void main(String[] args) {

    FileOutputStream out = null;

    FileOutputStream outSTr = null;

    BufferedOutputStream Buff = null;

    FileWriter fw = null;

    int count = 1000;// 写文件行数

    try {

      outSTr = new FileOutputStream(new File("./src/main/resources/uiddata.csv"));

      Buff = new BufferedOutputStream(outSTr);

      long begin0 = System.currentTimeMillis();
      Buff.write("ID,date,name,phonetype,serialname,salary,country\n".getBytes());

      int idcount = 2000000;
      if (args != null && args.length > 0 && args[0] != null) {
        idcount = Integer.valueOf(args[0]);
      }
      int datecount = 30;
      int countrycount = 9;
      // int namecount =5000000;
      int phonetypecount = 10000;
      int serialnamecount = 50000;
      // int salarycount = 200000;
      Map<Integer, String> countryMap = new HashMap<Integer, String>();
      countryMap.put(1, "usa");
      countryMap.put(2, "uk");
      countryMap.put(3, "china");
      countryMap.put(4, "indian");
      countryMap.put(5, "japan");
      countryMap.put(6, "korea");
      countryMap.put(7, "russia");
      countryMap.put(8, "poland");
      countryMap.put(0, "canada");

      StringBuilder sb = null;
      int id;
      for (int i = idcount; i > 0; i--) {

        sb = new StringBuilder();
        id = 4000000 + i;
        sb.append(id).append(",");// id
        // sb.append(i==1 ? "france" : countryMap.get((i+1) %
        // countrycount)).append(",");
        sb.append("2015/8/" + (i % datecount + 1)).append(",");
        // sb.append(i==1 ? "france" : countryMap.get(i %
        // countrycount)).append(",");
        sb.append("name" + (1600000 + i)).append(",");// name
        sb.append("phone" + i % phonetypecount).append(",");
        // sb.append(i==1 ? "france" : countryMap.get((i+2) %
        // countrycount)).append(",");
        sb.append("serialname" + (100000 + i % serialnamecount)).append(",");// serialname
        sb.append(i + 500000).append(',');
        sb.append(i == 1 || i == 2 ? "france" : countryMap.get((i + 3) % countrycount))
            .append('\n');
        // sb.append("name1" + (i + 100000)).append(",");// name
        // sb.append("name2" + (i + 200000)).append(",");// name
        // sb.append("name3" + (i + 300000)).append(",");// name
        // sb.append("name4" + (i + 400000)).append(",");// name
        // sb.append("name5" + (i + 500000)).append(",");// name
        // sb.append("name6" + (i + 600000)).append(",");// name
        // sb.append("name7" + (i + 700000)).append(",");// name
        // sb.append("name8" + (i + 800000)).append(",").append('\n');
        // System.out.println("sb.toString():" + sb.toString());
        Buff.write(sb.toString().getBytes());
        if (id == 4000001) {
          System.out.println("sb.toString():" + sb.toString());
        }
      }

      Buff.flush();

      Buff.close();
      System.out.println("sb.toString():" + sb.toString());
      long end0 = System.currentTimeMillis();

      System.out.println("BufferedOutputStream执行耗时:" + (end0 - begin0) + " 豪秒");

    } catch (Exception e) {

      e.printStackTrace();

    }

    finally {

      try {

        // fw.close();

        Buff.close();

        outSTr.close();

        // out.close();

      } catch (Exception e) {

        e.printStackTrace();

      }

    }

  }

}
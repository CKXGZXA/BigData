import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import hbase.api.HbaseUtil;

public class Hbase3_2_2 {
    public static void main(String[] args) {
        BufferedReader studentReader;
		try {
			studentReader = new BufferedReader(new FileReader("/home/hadoop/hbase/src/data/7.txt"));
			String line = studentReader.readLine();
			while (line != null) {
				// 将line字符串按照空格分割
                String[] strs = line.split(" ");

                String rowKey = strs[0];
                // 将数据写入Hbase
                HbaseUtil.putRow("Student", rowKey, "information", "name", strs[1]);
                HbaseUtil.putRow("Student", rowKey, "information", "sex", strs[2]);
                HbaseUtil.putRow("Student", rowKey, "information", "age", strs[3]);
                HbaseUtil.putRow("Student", rowKey, "stat_score", "sum", "NILL");
                HbaseUtil.putRow("Student", rowKey, "stat_score", "avg", "NILL");
				line = studentReader.readLine();
			}
			studentReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

        BufferedReader SCReader;
        try {
            SCReader = new BufferedReader(new FileReader("/home/hadoop/hbase/src/data/8.txt"));
            String line = SCReader.readLine();
            while (line != null) {
                // 将line字符串按照空格分割
                String[] strs = line.split(" ");

                String rowKey = strs[0];
                String qualifier = strs[1];
                String value = strs[2];
                // 将数据写入Hbase
                HbaseUtil.putRow("Student", rowKey, "score", qualifier, value);
                line = SCReader.readLine();
            }
                
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }

    }
}

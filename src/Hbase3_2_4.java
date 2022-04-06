import org.apache.hadoop.hbase.client.Table;

import hbase.api.HbaseConn;
import hbase.api.HbaseUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class Hbase3_2_4 {
    public static void addRecord(String tableName, String row, String[] fields, String[] values) {
        // 遍历fields
        for(int i = 0; i < fields.length; i++) {
            // 以:分割field
            String[] fieldSplit = fields[i].split(":");
            // 如果fieldSplit.length != 1, 说明field中包含列限定符
            if(fieldSplit.length != 1)
                HbaseUtil.putRow(tableName, row, fieldSplit[0], fieldSplit[1], values[i]);
            else {
                HbaseUtil.putRow(tableName, row, fieldSplit[0], values[i]);
            }
        }
    }

    /**
     * 得到某列数据
     * @param tableName
     * @param column
     * @return
     */
    public static boolean scanColumn(String tableName, String column) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            String[] data = column.split(":");
            if(data.length == 1) {
                scan.addFamily(column.getBytes());
            } else {
                scan.addColumn(data[0].getBytes(), data[1].getBytes());
            }
            ResultScanner rs = table.getScanner(scan);
            for (Result res : rs) {
                System.out.println(new String(res.getRow(), StandardCharsets.UTF_8));
                List<Cell> cells = res.listCells();
                for (Cell cell : cells) {
                    System.out.println(new String(cell.getValue(), StandardCharsets.UTF_8));
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }   
}

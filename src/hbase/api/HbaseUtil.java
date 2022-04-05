package hbase.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseUtil {
    /**
     * 创建表
     * @param tableName 表名
     * @param cfs 列族名
     * @return
     */
    public static boolean createTable(String tableName, String[] cfs) {
        try (HBaseAdmin admin = (HBaseAdmin)(HbaseConn.getHbaseConn()).getAdmin()) {
            if (admin.tableExists(tableName)) {
                return false;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor columnDesc = new HColumnDescriptor(cf);
                tableDesc.addFamily(columnDesc);
            }
            admin.createTable(tableDesc);
        } catch (IOException e) {
            //TODO: handle exception
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除表
     * @param tableName 表名
     * @return
     */
    public static boolean deleteTable(String tableName) {
        try (HBaseAdmin admin = (HBaseAdmin)HbaseConn.getHbaseConn().getAdmin()) {
            if (!admin.tableExists(tableName)) {
                return false;
            }
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            return true;
        } catch (Exception e) {
            //TODO: handle exception
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 插入数据
     * @param tableName 表名
     * @param rowKey 行键
     * @param cf 列族
     * @param qualifier 列名
     * @param value 值
     * @return
     */
    public static boolean putRow(String tableName, String rowKey, String cf, String qualifier, String value) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Put put = new Put(Bytes.toBytes(rowKey));   //行键
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 批量插入数据
     * @param tableName
     * @param puts
     * @return
     */
    public static boolean putRows(String tableName, List<Put> puts) {
        try (Table table = HbaseConn.getTable(tableName)) {
            table.put(puts);
        } catch (Exception e) {
            e.printStackTrace();
        } 
        return true;
    }

    /**
     * 查询单条数据
     * @param tableName 表名
     * @param rowKey 行键
     * @return
     */
    public static Result getRow(String tableName, String rowKey) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除行
     * @param tableName 表名
     * @param rowKey 行键
     * @return
     */
    public static boolean deleteRow(String tableName, String rowKey) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列族
     * @param tableName 表名
     * @param cf 列族
     * @return
     */
    public static boolean deleteColumnFamily(String tableName, String cf) {
        try (HBaseAdmin admin = (HBaseAdmin)HbaseConn.getHbaseConn().getAdmin()) {
            admin.deleteColumn(tableName, cf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 删除列
     * @param tableName 表名
     * @param rowKey 行键
     * @param cf 列族
     * @param qualifier 列名
     * @return
     */
    public static boolean deleteQualifier(String tableName, String rowKey, String cf, String qualifier) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(qualifier));
            table.delete(delete);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    /**
     * 扫描数据
     * @param tableName 表名
     * @param startRow 起始行键
     * @param stopRow 结束行键
     * @return
     */
    public static void Scan(String tableName, String startRow, String stopRow) {
        try (Table table = HbaseConn.getTable(tableName)) {
            Scan scan = new Scan();
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(Bytes.toBytes(stopRow));
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                // 输出行键
                System.out.print(new String(r.getRow(), StandardCharsets.UTF_8) + " ");
                // 得到单元格集合
                List<Cell> cells = r.listCells();
                for (Cell cell : cells) {
                    // getValue并转成utf-8
                    System.out.print(new String(cell.getValue(), StandardCharsets.UTF_8) + " ");
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        // return null;
    }
}

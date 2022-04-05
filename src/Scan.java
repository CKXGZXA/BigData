import java.util.Scanner;

import hbase.api.HbaseUtil;

public class Scan {
    public static void main(String[] args) {
        // System.out.println("请输入表名, 起始行键, 结束行键");
        // Scanner scanner = new Scanner(System.in);
        // String tableName = scanner.nextLine();
        // String startRow = scanner.nextLine();
        // String stopRow = scanner.nextLine();
        // HbaseUtil.Scan(tableName, startRow, stopRow);
        HbaseUtil.Scan(args[0], args[1], args[2]);
        // scanner.close();
    }
}
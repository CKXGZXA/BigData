import hbase.api.HbaseUtil;

public class InsertFirstLine {
    public static void main(String[] args) {
        if(HbaseUtil.deleteTable("Student")){
            System.out.println("删除表成功");
        }
        String[] cfs = {"information", "score", "stat_score"};
        HbaseUtil.createTable("Student", cfs);
        HbaseUtil.putRow("Student", "学号", "information", "name", "姓名");
        HbaseUtil.putRow("Student", "学号", "information", "sex", "性别");
        HbaseUtil.putRow("Student", "学号", "information", "age", "年龄");
        HbaseUtil.putRow("Student", "学号", "score", "123001", "成绩");
        HbaseUtil.putRow("Student", "学号", "score", "123002", "成绩");
        HbaseUtil.putRow("Student", "学号", "score", "123003", "成绩");
        HbaseUtil.putRow("Student", "学号", "stat_score", "sum", "总成绩");
        HbaseUtil.putRow("Student", "学号", "stat_score", "avg", "平均成绩");
        // 调用Scan方法
        HbaseUtil.Scan("Student", "学号", "学号");
    }
}

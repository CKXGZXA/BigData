import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class FileSort {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: FileSort <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "FileSort");
        job.setJarByClass(FileSort.class);
        job.setMapperClass(FileSortMapper.class);
        job.setReducerClass(FileSortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输入文件路径
        for(int i= 0;i<otherArgs.length-1;i++){
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        // 设置输出文件路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
        // 如果输出目录有文件，则删除
        Path outputPath = new Path(otherArgs[otherArgs.length-1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        // 提交任务
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

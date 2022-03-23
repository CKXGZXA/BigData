import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FileSortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{

    @Override
    protected void map(Object key, Text value,
            Mapper<Object, Text, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        // 将value变为key, 输出
        context.write(new IntWritable(Integer.parseInt(value.toString())), new IntWritable(1));
    }
}

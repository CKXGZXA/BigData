import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FileSortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{

    private static IntWritable num = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> value,
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        for(IntWritable val : value){
            context.write(num , key);
            num.set(num.get() + 1);
        }
    }
}

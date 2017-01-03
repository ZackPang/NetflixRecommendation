/**
 * Created by zackpeng on 10/4/16.
 */
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*      全套：
        input一行长这样：
        userID, movieID, rating
        1,10001,5.0
        1,10002,3.0
        1,10003,2.5
        2,10001,2.0
        2,10002,2.5
        2,10003,5.0
        2,10004,2.0
        3,10001,2.0
        3,10004,4.0
        3,10005,4.5
        3,10007,5.0
        4,10001,5.0
        4,10003,3.0
        4,10004,4.5
        4,10006,4.0
        5,10001,4.0
        5,10002,3.0
        5,10003,2.0
        5,10004,4.0
        5,10005,3.5
        5,10006,4.0


        output:
        key          value
        userID       movieID: rating, movieID: rating, ...  关键：这个mapper或reducer都不sort values里面的movieID
        1            10001:5.0,10002:3.0,10003:2.5
        2            10001:2.0,10002:2.5,10003:5.0,10004:2.0 ...
        .            ...
        .            ...
        .            ...
        */


public class A_DataDividerByUser {
    public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        /*
        input一行长这样：
        userID, movieID, rating
        1,10001,5.0
        1,10002,3.0
        1,10003,2.5
        2,10001,2.0
        2,10002,2.5
        2,10003,5.0
        2,10004,2.0
        3,10001,2.0
        3,10004,4.0
        3,10005,4.5
        3,10007,5.0
        4,10001,5.0
        4,10003,3.0
        4,10004,4.5
        4,10006,4.0
        5,10001,4.0
        5,10002,3.0
        5,10003,2.0
        5,10004,4.0
        5,10005,3.5
        5,10006,4.0


        output一行长这样：
        userID       movieID: rating
        1            10001:5.0
        1            10002:3.0
        1            10003:2.5
        2            10001:2.0
        2            10002:2.5
        2            10003:5.0
        2            10004:2.0
        .
        .
        .
         */

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] eachLine = value.toString().trim().split(",");
            Integer userID = Integer.parseInt(eachLine[0]);
            String movieID = eachLine[1];
            String rating = eachLine[2];

            context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
        }
    }


    public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        /*
        input:
        userID       movieID: rating
        1            10001:5.0
        1            10002:3.0
        1            10003:2.5
        2            10001:2.0
        2            10002:2.5
        2            10003:5.0
        2            10004:2.0
        .
        .
        .

        output:
        key          value
        userID       movieID: rating, movieID: rating, ...  关键：这个mapper或reducer都不sort values里面的movieID
        1            10001:5.0,10002:3.0,10003:2.5
        2            10001:2.0,10002:2.5,10003:5.0,10004:2.0 ...
        .            ...
        .            ...
        .            ...
        */

        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //就调整下values
            StringBuilder sb = new StringBuilder();
            Iterator<Text> it = values.iterator();
            while (it.hasNext()){
                sb.append("," + it.next());
            }
            String value = sb.toString().replaceFirst(",", "");

            context.write(key, new Text(value));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDividerMapper.class);
        job.setReducerClass(DataDividerReducer.class);
        job.setJarByClass(A_DataDividerByUser.class);

        //job.setNumReduceTasks(10);

        //设置整个mapper-reducer过程的input和output的format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //这个默认了mapper和reducer的inputFormatClass都是一样的。
        //因为这边就是一样的。如果不一样，要提前再设置一个mapperInputFormatClass/mapperOutputFormatClass
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextInputFormat.setInputPaths(job, new Path(args[1]));

        //true: 第二个job等第一个job结束后才开始
        job.waitForCompletion(true);
    }

}

//然后拿输出结果来build co-occurance matrix
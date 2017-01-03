/**
 * Created by zackpeng on 10/5/16.
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

/*
全套：

input:
key          value
            userID  movieIDb: rating, movieIDb: rating, ...  关键：这个mapper或reducer都不sort values里面的movieID
            1       10001:5.0,10002:3.0,10003:2.5
            2       10001:2.0,10002:2.5,10003:5.0,10004:2.0 ...
            .       ...
            .       ...
            .       ...


output:
key                         value
movieIDa:movieIDb
10001:10001                 1
10001:10002                 1
10001:10003                 1
10002:10001                 1
10002:10002                 1
10002:10003                 1
10003:10001                 1
10003:10002                 1
10003:10003                 1

10001:10001                 1
10001:10002                 1
10001:10003                 1
10001:10004                 1
10002:10001                 1
10002:10002                 1
10002:10003                 1
10002:10004                 1
10003:10001                 1
10003:10002                 1
10003:10003                 1
10003:10004                 1
10004:10001                 1
10004:10002                 1
10004:10003                 1
10004:10004                 1
.
.
.


after reducer
==============>>
key                         value
movieIDa:movieIDb
10001:10001                 2
10001:10002                 2
10001:10003                 2
10001:10004                 1
10002:10001                 2
10002:10002                 2
10002:10003                 2
10002:10004                 1
10003:10001                 2
10003:10002                 2
10003:10003                 2
10003:10004                 1
10004:10001                 1
10004:10002                 1
10004:10003                 1
10004:10004                 1
.
.
.
*/

public class B_CoOccurrenceMatrixGenerator {

    public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyAndValues = value.toString().trim().split("\t");
            String Values = keyAndValues[1];
            String[] valuesArr = Values.split(",");

            for (int i = 0; i < valuesArr.length; i++) {
                String movieA = valuesArr[i].trim().split(":")[0].trim();

                for (int j = 0; j < valuesArr.length; j++) {
                    String movieB = valuesArr[j].trim().split(":")[0].trim();
                    context.write(new Text(movieA + ":" + movieB), new IntWritable(1));
                }
            }
        }
    }

    public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            Iterator<IntWritable> it = values.iterator();
            int sum = 0;
            while (it.hasNext()){
                sum += it.next().get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setMapperClass(MatrixGeneratorMapper.class);
        job.setReducerClass(MatrixGeneratorReducer.class);
        job.setJarByClass(B_CoOccurrenceMatrixGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

    }
}

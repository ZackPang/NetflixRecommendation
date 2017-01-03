/**
 * Created by zackpeng on 10/5/16.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

拿到的matrix:
        key                       value
        movie1 : movie2     \t    relation

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

load进RAM：
即matrix的一行：
        [10001: {10001, 10002, 8.0}{10001, 10003, 5.0}{10001, 10003, 6.0}]
        [10002: {10002, 10001, 8.0}{10002, 10005, 9.0}{10002, 10009, 10.0}]


hashmap2, Sum.
        <movieNumber, matrix每一行的sum>:
        <K,     V>
        <movie1,7>
        <movie2,7>
        <movie3,7>
        <movie4,4>





input1: User1's score input file，一行一行扫的。
        key         value
        null        userID, movieID看过这部, rating
        null        userID, movieID看过这部, rating
        null        userID, movieID看过这部, rating
        null        userID, movieID没看这部, 0
        null        userID, movieID没看这部, 0



output:
        key                                     value
        userID                                  movieTag(which movie2) : score SUM UP
        1                                        10001:20／7 + 16／7 + 2 = 7.14
        1                                        10002:20／7 + 16／7 + 2 = 7.14
        1                                        10003:20／7 + 16／7 + 2 = 7.14
        1                                        10004:10／7 + 8／7 + 2 = 4.57

        2


*/



public class C_Multiplication {

    /*
    1.同时做归一化处理
    2.load matrix，相乘
    */
    public static class MultiplicationMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        /*
        拿到的matrix:
        key                       value
        movie1 : movie2     \t    relation

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

        1.setup().统计每行sum，做normalization.

        a.
        自己写了个class来指代这个关系(movie1:movie2   1):
        class MovieRelation {
            private int movie1;
            private int movie2;
            private int relation;
        }

        b.2个hashmap。
        理想中，我要这样的一个表，来求和。
        [10001: {10001, 10002, 8.0}{10001, 10003, 5.0}{10001, 10003, 6.0}]
        [10002: {10002, 10001, 8.0}{10002, 10005, 9.0}{10002, 10009, 10.0}]
        所以，hashmap1,先存全matrix？？？？？？？？这里不明白。这不爆了RAM？
        */
        Map<Integer, List<C1_MovieRelation>> movieRelationMap = new HashMap<>();

        /*现在就可以求和了。
        hashmap2
        <movieNumber, matrix每一行的sum>:
        <K,     V>
        <movie1,7>
        <movie2,7>
        <movie3,7>
        <movie4,4>
        .
        .
        .
        */
        Map<Integer, Integer> movie_sum_Map = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            //1.load matrix == 建立第一个hashmap


            //读文件过程： Path, FileSystem --> BufferedReader --> readLine()
            /*
            driver中：
            conf.set("coOccurrencePath", args[0]);
             */
            Configuration conf = new Configuration();
            String filePath = conf.get("coOccurencePath");

            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();

            while (line != null) {
                String[] tokens = line.toString().trim().split("\t");
                String[] moviesArr = tokens[0].trim().split(":");

                int movie1ID = Integer.parseInt(moviesArr[0].trim());
                int movie2ID = Integer.parseInt(moviesArr[1].trim());
                int relation = Integer.parseInt(tokens[1].trim());

                if (movieRelationMap.containsKey(movie1ID)) {
                    C1_MovieRelation movieRelation = new C1_MovieRelation(movie1ID, movie2ID, relation);
                    movieRelationMap.get(movie1ID).add(movieRelation);
                } else {
                    List<C1_MovieRelation> list = new ArrayList<>();
                    C1_MovieRelation movieRelation = new C1_MovieRelation(movie1ID, movie2ID, relation);
                    list.add(movieRelation);
                    movieRelationMap.put(movie1ID, list);
                }

                line = br.readLine();
            }

            br.close();


            //2. 归一化summation
            for (Map.Entry<Integer, List<C1_MovieRelation>> eachEntry: movieRelationMap.entrySet()) {
                int sum = 0;
                for (C1_MovieRelation eachInList : eachEntry.getValue()){
                    sum += eachInList.getRelation();
                }
                movie_sum_Map.put(eachEntry.getKey(),sum);
            }
        }




        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            /*
            input: User1's score input file，一行一行扫的。
            key         value
            null        userID, movieID看过这部, rating
            null        userID, movieID看过这部, rating
            null        userID, movieID看过这部, rating
            null        userID, movieID没看这部, 0
            null        userID, movieID没看这部, 0


            input file的一行 * matrix的一列。


            output:
            key                                     value
            userID:  movie2                         相乘以后的score
            1:10001                                 20/7            <-1:10001:10.0 乘完
            1:10002                                 20/7
            1:10003                                 20/7
            1:10004                                 10/7

            1:10001                                 16/7            <-1:10002:8.0 乘完
            1:10002                                 16/7
            1:10003                                 16/7
            1:10004                                 8/7

            1:10001                                 2               <-1:10001:7.0 乘完
            1:10002                                 2
            1:10003                                 2
            1:10004                                 2

            ..
            ..

            接着user2的相乘的部分


            */
            String[] tokens = value.toString().trim().split("\t");
            //String[] user_rating = tokens[1].split(":");

            int user = Integer.parseInt(tokens[0]);
            int movie = Integer.parseInt(tokens[1]);
            double rating = Double.parseDouble(tokens[2]);

            //movie1 * relation系数 = movie2的评分
            for (C1_MovieRelation relation : movieRelationMap.get(movie)) {
                double score = rating * relation.getRelation();

                //normalized
                score /= movie_sum_Map.get(relation.getMovie2());

                DecimalFormat df = new DecimalFormat("#.00");
                score = Double.valueOf(df.format(score));

                context.write(new Text(user + ":" + relation.getMovie2()), new DoubleWritable(score));
            }
        }
    }








    public static class MultiplicationReducer extends Reducer<Text, DoubleWritable, IntWritable, Text> {
        /* reduce method
        input:
        key                                     value
        userID:  movie2                         相乘以后的score
        1:10001                                 20/7            <-1:10001:10.0 乘完
        1:10002                                 20/7
        1:10003                                 20/7
        1:10004                                 10/7

        1:10001                                 16/7            <-1:10002:8.0 乘完
        1:10002                                 16/7
        1:10003                                 16/7
        1:10004                                 8/7

        1:10001                                 2               <-1:10001:7.0 乘完
        1:10002                                 2
        1:10003                                 2
        1:10004                                 2

        ..
        ..

        接着user2的相乘的部分

        output:
        key                                     value
        userID                                  movieTag(which movie2) : score SUM UP
        1                                        10001:20／7 + 16／7 + 2 = 7.14
        1                                        10002:20／7 + 16／7 + 2 = 7.14
        1                                        10003:20／7 + 16／7 + 2 = 7.14
        1                                        10004:10／7 + 8／7 + 2 = 4.57
        */

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            Iterator<DoubleWritable> it = values.iterator();
            while (it.hasNext()) {
                sum += it.next().get();
            }

            String[] keyArr = key.toString().trim().split(":");
            int user = Integer.parseInt(keyArr[0].trim());
            int movie2ID = Integer.parseInt(keyArr[1].trim());

            context.write(new IntWritable(user), new Text(movie2ID + ":" + sum));
        }
    }






    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("coOccurrencePath", args[0]);

        Job job = Job.getInstance();
        job.setMapperClass(MultiplicationMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setJarByClass(C_Multiplication.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[1]));
        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}

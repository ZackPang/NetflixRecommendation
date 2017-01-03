/**
 * Created by zackpeng on 10/7/16.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
目的：
        1. filter out watched movie.  (把watching history load进hashmap里。)
           mapper 做。
        2. match movie_name to movie_id
           reducer做。

input:
        key                                     value
        userID                                  movieTag(which movie2) : score SUM UP
        1                                        10001:20／7 + 16／7 + 2 = 7.14 （watched)
        1                                        10002:20／7 + 16／7 + 2 = 7.14 （watched)
        1                                        10003:20／7 + 16／7 + 2 = 7.14 （watched)
        1                                        10004:10／7 + 8／7 + 2 = 4.57  （unwatched)


output:
        key         value
        1       "The Rock":4.57


mapper做filter.
        mapper output:
        1                                        10004:4.57  （unwatched)

reducer做match:
        把output:
            1       10004:4.57

            写成

            1       "The Rock":4.57

*/


public class D_RecommenderListGenerator {
    public static class RecommenderListGeneratorMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        /*
        input:
        key                                     value
        userID                                  movieTag(which movie2) : score SUM UP
        1                                        10001:20／7 + 16／7 + 2 = 7.14
        1                                        10002:20／7 + 16／7 + 2 = 7.14
        1                                        10003:20／7 + 16／7 + 2 = 7.14
        1                                        10004:10／7 + 8／7 + 2 = 4.57

         */


        //把watching history load进hashmap里。
        // K: userID, V: movieID
        Map<Integer, List<Integer>> watchHistory = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            //read data from watchHistory(original data) file to this watchHistory HashMap.
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("watchHistory");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();

            //WatchHistory is original data.
            //Its format is:
            //userID,movieID,ratingScore

            while(line != null) {
                int user = Integer.parseInt(line.split(",")[0]);
                int movie = Integer.parseInt(line.split(",")[1]);
                if(watchHistory.containsKey(user)) {
                    watchHistory.get(user).add(movie);
                }
                else {
                    List<Integer> list = new ArrayList<>();
                    list.add(movie);
                    watchHistory.put(user, list);
                }
                line = br.readLine();
            }
            br.close();

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            input:

            key                                     value
            userID                                  movieTag(which movie2) : score SUM UP
            1                                        10001:20／7 + 16／7 + 2 = 7.14
            1                                        10002:20／7 + 16／7 + 2 = 7.14
            1                                        10003:20／7 + 16／7 + 2 = 7.14
            1                                        10004:10／7 + 8／7 + 2 = 4.57

            //output == input. Just filter out watched history
            key                                     value
            userID                                  movieTag(which movie2) : score SUM UP
            1                                        10004:4.57
            */

            String[] tokens = value.toString().split("\t");
            int user = Integer.parseInt(tokens[0]);
            int movie = Integer.parseInt(tokens[1].split(":")[0]);
            if(!watchHistory.get(user).contains(movie)) {
                context.write(new IntWritable(user), new Text(movie + ":" + tokens[1].split(":")[1]));
            }
        }
    }

    public static class RecommenderListGeneratorReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        /*
        1.load movieid和movieName的表。
        2.把input:
            key     value
            1       10004:4.57

            写成
            1       "The Rock":4.57

         */


        //match movie_name to movie_id
        Map<Integer, String> movieTitles = new HashMap<>();

        //在setup步骤中，load一个movieID，movieName的KV pair的文件。
        //实际公司中，这些不可能是存在txt里，肯定在db中。

        @Override
        protected void setup(Context context) throws IOException {
            //<movie_id, movie_title>
            //read movie title from file
            Configuration conf = context.getConfiguration();
            String filePath = conf.get("movieTitles");
            Path pt = new Path(filePath);
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line = br.readLine();
            //movieid,movie_name
            while (line != null) {
                int movie_id = Integer.parseInt(line.trim().split(",")[0]);
                movieTitles.put(movie_id, line.trim().split(",")[1]);
                line = br.readLine();
            }
            br.close();
        }

        // reduce method
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //movie_id:rating
            while (values.iterator().hasNext()) {
                String cur = values.iterator().next().toString();
                int movie_id = Integer.parseInt(cur.split(":")[0]);
                String rating = cur.split(":")[1];

                context.write(key, new Text(movieTitles.get(movie_id) + ":" + rating));
            }
        }
    }




    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("watchHistory", args[0]);
        conf.set("movieTitles", args[1]);

        Job job = Job.getInstance(conf);
        job.setMapperClass(RecommenderListGeneratorMapper.class);
        job.setReducerClass(RecommenderListGeneratorReducer.class);

        job.setJarByClass(D_RecommenderListGenerator.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[2]));
        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        job.waitForCompletion(true);
    }
}
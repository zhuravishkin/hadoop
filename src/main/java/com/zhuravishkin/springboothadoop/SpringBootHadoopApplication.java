package com.zhuravishkin.springboothadoop;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.StringTokenizer;

@Slf4j
@SpringBootApplication
public class SpringBootHadoopApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringBootHadoopApplication.class, args);
        System.setProperty("hadoop.home.dir", "C:/winutils/");
        System.setProperty("HADOOP_USER_NAME", "hduser");
        Configuration conf = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(conf, "wordCount");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        job.setJarByClass(SpringBootHadoopApplication.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        try {
            FileInputFormat.addInputPath(job, new Path("hdfs://192.168.0.12:9000/user/user/input"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.0.12:9000/user/user/output"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}

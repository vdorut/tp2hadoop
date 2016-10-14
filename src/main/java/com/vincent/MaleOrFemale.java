package com.vincent;

/**
 * Created by vincent on 12/10/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaleOrFemale {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text origin = new Text();


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {


            try {

                String[] line = value.toString().split(";");
                String[] origins = line[1].split(","); // you can have multiple origin separated by a comma

                for (String i : origins) {
                    origin.set(i);
                    context.write(origin, one);
                }

                context.write(origin, one);
            }
            catch(Exception e){

            }



        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MaleOrFemale");
        job.setJarByClass(MaleOrFemale.class);
        job.setMapperClass(MaleOrFemale.TokenizerMapper.class);
        job.setCombinerClass(MaleOrFemale.IntSumReducer.class);
        job.setReducerClass(MaleOrFemale.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

package com.udbac.ua.mr;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by root on 2017/2/14.
 */
public class AslogUnique {

    public static class AslogUniqueMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static Set<String> uaSet = new HashSet<>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).increment(1);
            String[] tokens = StringUtils.split(value.toString(), "\t");
            if (tokens.length != 12) {
                return;
            }
            String uaString = tokens[8];
            String uaHash = UAHashUtils.hashUA(uaString);
            if (uaSet.contains(uaHash)) {
                return;
            }
            uaSet.add(uaHash);
            String parsedUA = UAHashUtils.handleUA(uaString);
            context.write(new Text(UAHashUtils.hashUA(parsedUA) + "\t" + parsedUA ), NullWritable.get());
        }
    }

    public static class AslogUniqueReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        try {
            Configuration conf = new Configuration();
            conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");

            String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
            if (inputArgs.length != 2) {
                System.err.println("\"Usage:<inputPath> <ouputPath>/n\"");
                System.exit(2);
            }
            String inputPath = inputArgs[0];
            String outputPath = inputArgs[1];

            Job job1 = Job.getInstance(conf, "UniqueUA");
            job1.setJarByClass(AslogUnique.class);
            job1.setMapperClass(AslogUniqueMapper.class);
            job1.setReducerClass(AslogUniqueReduce.class);
            TextInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));
            LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
            TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(NullWritable.class);

            if (job1.waitForCompletion(true)) {
                System.out.println((System.currentTimeMillis() - starttime) / 1000);
                System.out.println("-----alllines count-----:" +
                        job1.getCounters().findCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).getValue());
            } else {
                System.exit(1);
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("*****job failed*****");
        }
    }
}

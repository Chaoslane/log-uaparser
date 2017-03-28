package com.udbac.ua.mr;

import com.udbac.ua.util.RegexFilter;
import com.udbac.ua.util.UAHashUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by root on 2017/2/14.
 */
public class AslogUnique {

    public static class AslogUniqueMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private static Set<String> uagnset = new HashSet<>(1024*1024);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).increment(1);
            String[] tokens = value.toString().split("[\t]");
            int len = tokens.length;
            String uagn = null;
            if (len == 10) {
                uagn = tokens[8];
            } else if (len == 11 || len == 12) {
                if (StringUtils.isBlank(tokens[4])) {
                    System.out.println("Got null adid. Log:"+value.toString());
                    return;
                }
                uagn = tokens[8];
            }

            String uaid = null;
            String parsedUA =null;
            if (uagnset.contains(uagn)) {
                return;
            }
            uagnset.add(uagn);
            parsedUA = UAHashUtils.parseUA(uagn);
            uaid = UAHashUtils.hash(parsedUA);
            if (parsedUA.contains("\\x")) {
                parsedUA = URLDecoder.decode(parsedUA.replace("\\x", "%"), "UTF-8");
            }
            context.write(new Text(uaid+"\t"+parsedUA), NullWritable.get());
        }
    }

    public static class AslogUniqueReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
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
        FileInputFormat.setInputPathFilter(job1, RegexFilter.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        if (job1.waitForCompletion(true)) {
            System.out.println("-----succeed-----");
            long costTime = (job1.getFinishTime() - job1.getStartTime()) / 1000;
            long linesum = job1.getCounters().findCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s");
        } else {
            System.out.println("*****job failed*****");
            System.exit(1);
        }

    }
}

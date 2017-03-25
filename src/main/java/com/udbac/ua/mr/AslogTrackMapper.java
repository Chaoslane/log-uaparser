package com.udbac.ua.mr;

import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.UnsupportedlogException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by root on 2017/2/15.
 */
public class AslogTrackMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Logger logger = Logger.getLogger(AslogTrackMapper.class);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (StringUtils.isNotBlank(value.toString())) {
            try {
                Text text = new Text(AslogTrackParser.asLogParser(value.toString()));
                context.write(NullWritable.get(),text);
            } catch (UnsupportedlogException e) {
                logger.info(e.getMessage());
            }
        }
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("io.compression.codecs", "io.sensesecure.hadoop.xz.XZCodec");
        String inputArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (inputArgs.length != 2) {
            System.err.println("\"Usage:<inputPath> <outputPath>/n\"");
            System.exit(2);
        }
        String inputPath = inputArgs[0];
        String outputPath = inputArgs[1];

        Job job1 = Job.getInstance(conf, "TrackUA");
        job1.setJarByClass(AslogTrackMapper.class);
        job1.setMapperClass(AslogTrackMapper.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

        job1.setMapOutputKeyClass(NullWritable.class);
        job1.setMapOutputValueClass(Text.class);

        job1.setNumReduceTasks(0);

        if (job1.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
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

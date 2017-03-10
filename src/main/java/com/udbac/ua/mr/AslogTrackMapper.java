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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/2/15.
 */
public class AslogTrackMapper extends Mapper<LongWritable, Text, NullWritable,Text > {
    private static String[] vec;
    private static Map<String, String> ua_hash = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //读取日志中的指定字符串进行hash 有序
        vec = new String[]{"m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
                "m1", "m3", "m1b", "m9c", "mo"};
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = StringUtils.split(value.toString(), "\t");
        if (tokens.length != 12) {
            return;
        }
        String daytime = tokens[0].substring(0, 19).replace("T"," ");
        //处理UA串，并进行hash
        String uaStr = tokens[8];
        String parsedUA = null;
        String uaid = null;
        if (ua_hash.containsKey(uaStr)) {
            uaid = ua_hash.get(uaStr);
        }
        else{
            parsedUA = UAHashUtils.handleUA(uaStr);
            uaid = UAHashUtils.hashUA(parsedUA);
            ua_hash.put(uaStr, uaid);
        }
        //取得wxid并进行hash得到UDBACID
        String wxid = getWxid(tokens[5] + "," + tokens[6], tokens[10], tokens[2], tokens[8]);
        String wxided = UAHashUtils.hashUA(wxid);

        context.getCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).increment(1);
        context.write(NullWritable.get(),new Text( wxided + "\t" + daytime +"\t"+ uaid));
    }

    private static String getWxid(String aurl_aarg, String auid, String addr, String uagn) {
        String wxid = null;
        String[] querys = aurl_aarg.split("[,&]");
        Map<String, String> queryMap = new HashMap<>();
        for (String query : querys) {
            String[] kv = StringUtils.split(query, "=");
            if (kv.length == 2) {
                if (StringUtils.startsWith(kv[1], "__") && StringUtils.endsWith(kv[1], "__")) {
                    kv[1] = null;
                }
                queryMap.put(kv[0], kv[1]);
            }
        }

        for (String ve : vec) {
            if (StringUtils.isNotBlank(queryMap.get(ve))) {
                wxid = queryMap.get(ve);
                break;
            }
        }

        if (StringUtils.isBlank(wxid)) {
            if (auid.length() > 0) {
                wxid = auid;
            } else {
                wxid = addr + "#" + uagn;
            }
        }
        return wxid;
    }

    public static void main(String[] args) {
        long starttime = System.currentTimeMillis();
        try {
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

package com.udbac.ua.mr;

import com.udbac.ua.entity.Aslog;
import com.udbac.ua.entity.UAinfo;
import com.udbac.ua.util.LogParseException;
import com.udbac.ua.util.RegexFilter;
import com.udbac.ua.util.AsUtils;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/2/15.
 */
public class AslogTrackMapper {
    public enum MyCounters {ALLLINECOUNTER}

    static class TrackMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static Logger logger = Logger.getLogger(TrackMapper.class);
        //ua字符串和ua解析结果的map

        private String date;
        private Map<String, String> uaInfoMap;
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            date = context.getConfiguration().get("filename.pattern").replaceAll("[^0-9]", "");
            date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
            multipleOutputs = new MultipleOutputs<>(context);
            uaInfoMap = new HashMap<>(1024);
        }


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(MyCounters.ALLLINECOUNTER).increment(1);

            if (null == value) return;
            String line = value.toString();
            // 校验时间
            if (!date.equals(line.substring(0, 10))) return;

            try {
                // 日志转化为 Aslog 实体
                Aslog aslog = new Aslog(line);
                // 获取解析的uainfo字符串
                String uaInfo = new UAinfo().parseUagn(aslog.getUagn()).toString();

//                if (validArg(uaInfo)) {
//                    return;
//                }

                // hashid
                String hwxid = AsUtils.hash(AslogHandler.getWxid(aslog));
                String uaid = AsUtils.hash(uaInfo);
                String uaTrack = hwxid + "\t" +
                        aslog.getTime() + "\t" +
                        aslog.getAdid() + "\t" + uaid;
                //使用 adop 切分数据为 imp clk
                uaInfoMap.put(uaid, uaInfo);
                multipleOutputs.write(NullWritable.get(), new Text(uaTrack), aslog.getAdop());
            } catch (IllegalArgumentException | LogParseException e) {
                logger.warn(e);
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry entry : uaInfoMap.entrySet()) {
                Text uainfo = new Text(entry.getKey() + "\t" + entry.getValue());
                multipleOutputs.write(NullWritable.get(), uainfo, "uainfo");
            }
            multipleOutputs.close();
        }

//        public static boolean validArg(String str) {
//            String regex = "[ \tA-Za-z0-9/,._-]*";
//            Pattern pattern = Pattern.compile(regex);
//            Matcher matcher = pattern.matcher(str);
//            return matcher.matches();
//        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        long startTime = System.currentTimeMillis();
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
        job1.setMapperClass(TrackMapper.class);
        job1.setMapOutputKeyClass(NullWritable.class);

        TextInputFormat.setInputPathFilter(job1, RegexFilter.class);
        TextInputFormat.setInputPaths(job1, new Path(inputPath));

        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

        job1.setNumReduceTasks(0);

        if (job1.waitForCompletion(true)) {
            System.out.println("-----job succeed-----");
            long costTime = (System.currentTimeMillis() - startTime) / 1000;
            long linesum = job1.getCounters().findCounter(MyCounters.ALLLINECOUNTER).getValue();
            System.out.println(
                    linesum + " lines take:" + costTime + "s " + linesum / costTime + " line/s");
        } else {
            System.out.println("*****job failed*****");
            System.exit(1);
        }
    }
}

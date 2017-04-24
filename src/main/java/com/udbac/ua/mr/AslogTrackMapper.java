package com.udbac.ua.mr;

import com.udbac.ua.entity.AslogRaw;
import com.udbac.ua.util.RegexFilter;
import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.HashIdException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
 *
 */
public class AslogTrackMapper {

    static class TrackMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
        private static Logger logger = Logger.getLogger(TrackMapper.class);
        private static Map<String, String> uakv = new HashMap<>(1024 * 1024);
        private static String date;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            date = context.getConfiguration().get("filename.pattern").replaceAll("[^0-9]", "");
            date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(UAHashUtils.MyCounters.ALLLINECOUNTER).increment(1);
            AslogRaw aslog = AslogRaw.parseAslog(value.toString());
            if (null == aslog) return;
            if (!aslog.getTime().contains(date)){
                logger.warn("Got Illegal date :" + date);
                return;
            }

            try {
                //获取解析的uainfo字符串
                String uaInfo = null;
                String uagn = aslog.getUagn();
                if (StringUtils.isNotBlank(uakv.get(uagn))) {
                    uaInfo = uakv.get(uagn);
                } else {
                    uaInfo = UAHashUtils.parseUA(uagn);
                    uakv.put(uagn, uaInfo);
                }

                String hwxid = UAHashUtils.hash(AslogHandler.getWxid(aslog));
                String uaid = UAHashUtils.hash(uaInfo);

                MapWritable mw = new MapWritable();
                mw.put(new Text("wxid"), new Text(hwxid));
                mw.put(new Text("dt"), new Text(aslog.getTime()));
                mw.put(new Text("adid"), new Text(aslog.getAdid()));
                mw.put(new Text("uaid"), new Text(uaid));
                mw.put(new Text("uainfo"), new Text(uaInfo));

                context.write(new Text(aslog.getAdop()), mw);
            } catch (HashIdException e) {
                logger.warn(e);
            }
        }
    }


    static class TrackReducer extends Reducer<Text, MapWritable, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        private static Map<String, String> uaInfoMap ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<>(context);
            uaInfoMap = new HashMap<>();
        }

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            for (MapWritable value : values) {
                String wxid = value.get(new Text("wxid")).toString();
                String dt = value.get(new Text("dt")).toString();
                String adid = value.get(new Text("adid")).toString();
                String uaid = value.get(new Text("uaid")).toString();
                String uainfo = value.get(new Text("uainfo")).toString();

                uaInfoMap.put(uaid, uainfo);
                multipleOutputs.write(NullWritable.get(), new Text(wxid + "\t" + dt + "\t" + adid + "\t" + uaid), key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry entry : uaInfoMap.entrySet()) {
                multipleOutputs.write(NullWritable.get(), new Text(entry.getKey() + "\t" + entry.getValue()), "uainfo");
            }
            multipleOutputs.close();
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
        job1.setMapperClass(TrackMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MapWritable.class);
        job1.setReducerClass(TrackReducer.class);
        job1.setOutputKeyClass(NullWritable.class);

        TextInputFormat.setInputPathFilter(job1, RegexFilter.class);
        TextInputFormat.addInputPath(job1, new Path(inputPath));
        TextOutputFormat.setOutputPath(job1, new Path(outputPath));
        LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
        TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

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

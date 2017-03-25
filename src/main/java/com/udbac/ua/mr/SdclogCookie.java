package com.udbac.ua.mr;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 2017/2/17.
 */
public class SdclogCookie{

    public static class SdclogCookieMaaper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private String[] fields;
        private static Map<String, String> uaMap = new HashMap<>();
        @Override

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            fields = StringUtils.split(configuration.get("fields"), ",");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = StringUtils.split(value.toString(), " ");
            if (tokens.length != 15) {
                return;
            }
            String queryed = getquery(tokens[7], fields);
            String uaString = tokens[11].replaceAll("[+]", " ");
            String uaHash = UAHashUtils.hash(uaString);
            String uaDemension = null;
            if (uaMap.containsKey(uaHash)) {
                uaDemension = uaMap.get(uaHash);
            }else {
                uaDemension = UAHashUtils.parseUA(uaString);
                uaMap.put(uaHash, uaDemension);
            }
            context.write(new Text(queryed + "\t" + uaDemension), NullWritable.get());
        }

        public static String getquery(String queryStr, String[] fields) throws IOException {
            Map<String, String> queryMap = new HashMap<>();
            String[] querys = StringUtils.split(queryStr, "&");
            for (String query : querys) {
                String[] kv = StringUtils.split(query, "=");
                if (kv.length == 2) {
                    queryMap.put(kv[0], kv[1]);
                }
            }
            StringBuffer sb = new StringBuffer();
            for (String field : fields) {
                if (field.contains("?")) {
                    String[] fiesplits = StringUtils.split(field, "?");
                    for (String fiesplit : fiesplits) {
                        if (StringUtils.isNotBlank(queryMap.get(fiesplit))) {
                            field = fiesplit;
                            break;
                        }
                    }
                }
                sb.append(queryMap.get(field)).append("\t");
            }
            return sb.substring(0, sb.length() - 1);
        }
    }

    public static class SdclogCookieReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
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

            Job job1 = Job.getInstance(conf, "SdcLogUA");
            job1.setJarByClass(SdclogCookie.class);
            job1.setMapperClass(SdclogCookieMaaper.class);
            job1.setReducerClass(SdclogCookieReducer.class);
            TextInputFormat.addInputPath(job1, new Path(inputPath));
            TextOutputFormat.setOutputPath(job1, new Path(outputPath));
            LazyOutputFormat.setOutputFormatClass(job1, TextOutputFormat.class);
            TextOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(NullWritable.class);
            job1.setNumReduceTasks(1);

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


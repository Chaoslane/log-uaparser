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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/2/15.
 */
public class AslogTrackMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private static Logger logger = Logger.getLogger(AslogTrackMapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (StringUtils.isNotBlank(value.toString())) {
            try {
                Text text = new Text(asLogParser(value.toString()));
                context.write(NullWritable.get(),text);
            } catch (UnsupportedlogException e) {
                logger.info(e.getMessage());
            }
        }
    }

    private static String[] array =new String[]{
            "m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
            "m1", "m3", "m1b", "m9c", "mo"};
    private static List<String> vec = new ArrayList<>(Arrays.asList(array));

    private static Map<String, String> ua_hash = new HashMap<>(1024 * 1024);

    private static boolean validArg(String str) {
        String regex = "[A-Za-z0-9,-]*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.matches()) {
            return true;
        }
        return false;
    }

    static String asLogParser(String line) throws UnsupportedlogException {
        String[] tokens = StringUtils.splitPreserveAllTokens(line, "\t");
        String time = null;
        String msec = null;
        String addr = null;
        String xfwd = null;
        String adid = null; // ADID有问题，均为空
        String aurl = null;
        String aarg = null;
        String areq = null;
        String uagn = null;
        String ckie = null;
        String auid = null;
        String refr = null;
        int len = tokens.length;
        if (len == 10) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
        } else if (len == 11) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            if (StringUtils.isBlank(adid)) {
                throw new UnsupportedlogException("Unsupported log format Exception : bad adid : '"+ adid +"'\n" +"Log : "+ line);
            }
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
            auid = tokens[10];
        } else if (len == 12) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            if (StringUtils.isBlank(adid)) {
                throw new UnsupportedlogException("Unsupported log format Exception : bad adid : '"+ adid +"'\n" +"Log : "+ line);
            }
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
            auid = tokens[10];
            refr = tokens[11];
        } else {
            throw new UnsupportedlogException("Unsupported log format Exception : bad fields length : "+ len+"\n" +"Log : "+ line);
        }

        //获取wxid
        String wxid = null;
        Map<String, String> infoMap = new HashMap<>();

        for (String ustr : new String[]{aurl, aarg}) {
            if (StringUtils.isNotBlank(ustr)) {
                String[] items = ustr.split("[,&]");
                for (String item : items) {
                    String key = StringUtils.substringBefore(item,"=");
                    String value = StringUtils.substringAfter(item, "=");
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)
                            && value.length() > 0
                            && validArg(value)
                            && !key.equals("uid")
                            && value.length() < 20) {
                        infoMap.put(key, value);
                    }
                }
            }
        }
        if (!infoMap.isEmpty()) {
            for (String ve : vec) {
                if (StringUtils.isNotBlank(infoMap.get(ve))) {
                    wxid = infoMap.get(ve);
                    break;
                }
            }
        }
        if (StringUtils.isBlank(wxid)) {
            if (StringUtils.isNotBlank(auid)) {
                wxid = auid;
            } else {
                wxid = addr + "#" + uagn;
            }
        }

        String daytime = time.substring(0, 19).replace("T", " ");
        String udbacID = UAHashUtils.hash(wxid);

        //生成uaid 如果ua_hash中存在 则直接取
        String uaid = null;
        String parsedUA =null;
        if (ua_hash.containsKey(uagn)) {
            uaid = ua_hash.get(uagn);
        } else {
            parsedUA = UAHashUtils.parseUA(uagn);
            uaid = UAHashUtils.hash(parsedUA);
            ua_hash.put(uagn, uaid);
        }
        return udbacID + "\t" + daytime + "\t" + uaid ;
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

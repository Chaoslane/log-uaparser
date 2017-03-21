package com.udbac.ua.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/3/17.
 */
public class RegexFilter extends Configured implements PathFilter {

    private Pattern pattern;
    private Configuration conf;
    private FileSystem fs;

    @Override
    public boolean accept(Path path) {
        try {
            this.conf = getConf();
            if (null != conf) {
                fs = FileSystem.get(conf);
                pattern = Pattern.compile(conf.get("filename.pattern"));
            }else {
                System.out.println("get conf is null");
                return false;
            }
            if (fs.isDirectory(path)) {
                return true;
            } else {
                Matcher m = pattern.matcher(path.toString());
                System.out.println("Is path : " + path.toString() + " matches "
                        + conf.get("filename.pattern") + " ? , " + m.matches());
                return m.matches();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
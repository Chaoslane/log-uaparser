package com.udbac.ua.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/3/17.
 */
public class RegexFilter extends Configured implements PathFilter {
    private static Logger logger = Logger.getLogger(RegexFilter.class);
    private Pattern pattern;
    private FileSystem fs;

    @Override
    public boolean accept(Path path) {
        try {
            fs = FileSystem.get(getConf());
            pattern = Pattern.compile(getConf().get("filename.pattern"));
            if (fs.isDirectory(path)) {
                return true;
            } else {
                Matcher m = pattern.matcher(path.toString());
                if (m.matches()) {
                    logger.info(path.toString()+" is matched");
                }
                return m.matches();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

}
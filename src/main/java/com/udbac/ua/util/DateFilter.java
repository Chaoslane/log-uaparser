package com.udbac.ua.util;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/3/17.
 */
public class DateFilter extends Configured implements PathFilter {
    private static Logger logger = Logger.getLogger(DateFilter.class);

    @Override
    public boolean accept(Path path) {
        try {
            String filedate = getConf().get("filename.date");
            String yesterday = getSpecifiedDayBefore(filedate);
            String fileReg = String.format(".*%s.*|.*%s_23.*", filedate, yesterday);

            FileSystem fs = FileSystem.get(getConf());
            Pattern pattern = Pattern.compile(fileReg);

            if (fs.isDirectory(path)) {
                return true;
            } else if (fs.getFileStatus(path).getLen() < 100) { // 空文件的len为32
                return false;
            } else {
                Matcher m = pattern.matcher(path.toString());
                if (m.matches()) {
                    logger.info(path.getName() + " is matched");
                }
                return m.matches();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private static String getSpecifiedDayBefore(String specifiedDay) {
        Calendar c = Calendar.getInstance();
        Date date = null;
        try {
            date = new SimpleDateFormat("yyyyMMdd").parse(specifiedDay);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        c.setTime(date);
        int day = c.get(Calendar.DATE);
        c.set(Calendar.DATE, day - 1);
        return new SimpleDateFormat("yyyyMMdd").format(c.getTime());
    }
}
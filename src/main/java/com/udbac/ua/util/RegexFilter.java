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
 * 正则过滤输入文件
 */
public class RegexFilter extends Configured implements PathFilter {
    private static Logger logger = Logger.getLogger(RegexFilter.class);

    @Override
    public boolean accept(Path path) {
        try {
            FileSystem fs = FileSystem.get(getConf());
            String fileReg = getConf().get("filename.pattern");
            Pattern pattern = Pattern.compile(fileReg);

            if (fs.isDirectory(path)) {
                return true;
                //空文件为32Bytes 会造成xz异常 过滤掉
            } else if (fs.getFileStatus(path).getLen() < 100) {
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

}
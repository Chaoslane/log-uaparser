package com.udbac.ua.util;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.*;

/**
 * Created by root on 2017/5/23.
 */
public class AsUtilsTest {
    @Test
    public void urlDecode() throws Exception {
//        String s = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; \\xE7\\x88\\xB1\\xE5\\xB8\\x86\\xE6\\xB5\\x8F\\xE8\\xA7\\x88\\xE5\\x99\\xA8; .NET CLR 2.0.50727; .NET4.0C)";
//        System.out.println(AsUtils.urlDecode(s));
        Pattern pattern = Pattern.compile("[ \tA-Za-z0-9.-]*");
        Matcher matcher = pattern.matcher("XPERXVJli5T7s7XVsIgo\tNormal\tChrome Mobile\t43.0.2357\tAndroid\t5.1\tOppo R9tm\tOppo\tR9tm");
        System.out.println(matcher.matches());
    }

}
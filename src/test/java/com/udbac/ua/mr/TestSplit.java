package com.udbac.ua.mr;

import org.apache.commons.lang.StringUtils;

/**
 * Created by root on 2017/3/25.
 */
public class TestSplit {
    public static void main(String[] args) {
        String s = "a\t&bbb\tc";
        String[] s1 = s.split("[\t&]");
        System.out.println(s1.length);
    }
}

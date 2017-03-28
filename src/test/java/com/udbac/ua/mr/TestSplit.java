package com.udbac.ua.mr;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by root on 2017/3/25.
 */
public class TestSplit {
    public static void main(String[] args) {
//        String s = "a\t&bbb\tc";
//        String[] s1 = s.split("[\t&]");
//        System.out.println(s1.length);

        String[] array =new String[]{
                "m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
                "m1", "m3", "m1b", "m9c", "mo"};
        List<String> vec = new ArrayList<String>(Arrays.asList(array));
        for (String a : vec) {
            System.out.println(a);
        }

    }
}

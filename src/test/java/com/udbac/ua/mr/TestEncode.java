package com.udbac.ua.mr;

import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.UnsupportedlogException;



/**
 * Created by root on 2017/4/7.
 */
public class TestEncode {
    public static void main(String[] args) throws UnsupportedlogException {
        String s = "aaaa_aaa-ff-_ddd";
        System.out.println(s.replaceAll("[-_]",""));
    }
}

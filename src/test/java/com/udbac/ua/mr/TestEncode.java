package com.udbac.ua.mr;

import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.UnsupportedlogException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


/**
 * Created by root on 2017/4/7.
 */
public class TestEncode {
    public static void main(String[] args) throws UnsupportedlogException, UnsupportedEncodingException {
        String s = "aaa\\bbb\\ccc%bbb";
        System.out.println(s.replaceAll("(\\\\x[A-Za-z0-9]{0,2})|\\\\|R%H|R%", ""));
    }
}

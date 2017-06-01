package com.udbac.ua.entity;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by root on 2017/5/23.
 */
public class UAinfoTest {
    @Test
    public void parseUAStr() throws Exception {
//        String a = "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; \\xE7\\x88\\xB1\\xE5\\xB8\\x86\\xE6\\xB5\\x8F\\xE8\\xA7\\x88\\xE5\\x99\\xA8; .NET CLR 2.0.50727; .NET4.0C)";
//        String s = a.replaceAll("(\\\\x[A-Za-z0-9]{2}+)|(%[A-Za-z0-9]{2})+", "");
//        UAinfo uAinfo = UAinfo.parseUagn(a);

        System.out.println("aaaaa(bbbb)".replaceAll("\\(.*\\)", ""));
    }

}
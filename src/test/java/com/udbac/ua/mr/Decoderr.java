package com.udbac.ua.mr;

import java.io.IOException;

/**
 * Created by root on 2017/3/21.
 */
public class Decoderr {
    public static void main(String[] args) throws IOException {
        String source = "\\xE8\\xA5\\xBF\\xE7\\x93\\x9C";
        String sourceArr[] = source.split("\\\\");
        byte[] byteArr = new byte[sourceArr.length - 1];
        for (int i = 1; i < sourceArr.length; i++) {
            Integer hexInt = Integer.decode("0" + sourceArr[i]);
            byteArr[i - 1] = hexInt.byteValue();
        }
        System.out.println(new String(byteArr, "UTF-8"));
    }
}

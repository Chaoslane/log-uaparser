package com.udbac.ua.mr;


import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.udbac.ua.util.UAHashUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by root on 2017/3/22.
 */
public class UAHashUtilsTest {

    public static void main(String[] args) throws IOException {

        List<String> strings = FileUtils.readLines(new File("E:\\idm_out.txt"), "UTF-8");
        Set<String> idmset = new HashSet<>();
        for (String line : strings) {
            Gson gson = new Gson();
            Udbac udbac = null;
            try {
                udbac = gson.fromJson(line, Udbac.class);
                idmset.add(udbac.getUDBACid());
            } catch (Exception e) {
            }
        }
        System.out.println(idmset.size());

        List<String> strings1 = FileUtils.readLines(new File("E:\\my_out.txt"), "UTF-8");
        Set<String> myset = new HashSet<>();
        for (String line : strings1) {
            String[] split = StringUtils.split(line, "\t");
            myset.add(split[0]);
        }
        System.out.println(myset.size());

//        List<String> strings2 = FileUtils.readLines(new File("E:\\unique_out.txt"), "UTF-8");
//        Set<String> uniqueset = new HashSet<>();
//        for (String line : strings2) {
//            String[] split = StringUtils.split(line, "\t");
//            uniqueset.add(split[0]);
//        }
//        System.out.println(uniqueset.size());

        int i = 0;
        for (String str : idmset) {
            if (!myset.contains(str)) {
                i++;
                System.out.println(str);
            }
        }
        System.out.println(i);
    }

    static class Udbac{
        private String UDBACid;
        private String UAID;
        private String ISO8601;

        public String getISO8601() {
            return ISO8601;
        }

        public void setISO8601(String ISO8601) {
            this.ISO8601 = ISO8601;
        }

        public String getUDBACid() {
            return UDBACid;
        }

        public void setUDBACid(String UDBACid) {
            this.UDBACid = UDBACid;
        }

        public String getUAID() {
            return UAID;
        }

        public void setUAID(String UAID) {
            this.UAID = UAID;
        }
    }
}
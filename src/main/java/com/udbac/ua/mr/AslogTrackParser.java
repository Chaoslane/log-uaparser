package com.udbac.ua.mr;

import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.UnsupportedlogException;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 2017/3/24.
 */
public class AslogTrackParser {
    private static List<String> vec = new ArrayList<>();
    static {
        vec.add(1,"m2");    // IDFA
        vec.add(2,"m1c");   // AndroidID
        vec.add(3,"m1a");   // md5(AndroidID)
        vec.add(4,"m9b");   // md5(eth0_mac) 保留【:】
        vec.add(5,"m9");    // md5(eth0_mac) 去除【:】
        vec.add(6,"m2a");   // OpenUDID
        vec.add(7,"uid");   // Weibo

        vec.add(8,"m1");    // md5(IMEI)
        vec.add(9,"m3");    // md5(DUID)
        vec.add(10,"m1b");  // AAID
        vec.add(11,"m9c");  // ODIN
        vec.add(12,"mo" );  // md5(DUID)
    }



    private static Map<String, String> ua_hash = new HashMap<>(1024 * 1024);

    private static boolean validArg(String str) {
        String regex = "[A-Za-z0-9,-]*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.matches()) {
            return true;
        }
        return false;
    }

    static String asLogParser(String line) throws UnsupportedlogException {
        String[] tokens = line.split("[\t]");
        String time = null;
        String msec = null;
        String addr = null;
        String xfwd = null;
        String adid = null; // ADID有问题，均为空
        String aurl = null;
        String aarg = null;
        String areq = null;
        String uagn = null;
        String ckie = null;
        String auid = null;
        String refr = null;
        int len = tokens.length;
        if (len == 10) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
        } else if (len == 11) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            if (StringUtils.isBlank(adid)) {
                throw new UnsupportedlogException("Unsupported log format Exception : bad adid : '"+ adid +"'\n" +"Log : "+ line);
            }
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
            auid = tokens[10];
        } else if (len == 12) {
            time = tokens[0];
            msec = tokens[1];
            addr = tokens[2];
            xfwd = tokens[3];
            adid = tokens[4];
            if (StringUtils.isBlank(adid)) {
                throw new UnsupportedlogException("Unsupported log format Exception : bad adid : '"+ adid +"'\n" +"Log : "+ line);
            }
            aurl = tokens[5];
            aarg = tokens[6];
            areq = tokens[7];
            uagn = tokens[8];
            ckie = tokens[9];
            auid = tokens[10];
            refr = tokens[11];
        } else {
            throw new UnsupportedlogException("Unsupported log format Exception : bad fields length : "+ len+"\n" +"Log : "+ line);
    }

        //获取wxid
        String wxid = null;
        Map<String, String> infoMap = new HashMap<>();

        for (String ustr : new String[]{aurl, aarg}) {
            if (StringUtils.isNotBlank(ustr)) {
                String[] items = ustr.split("[,&]");
                for (String item : items) {
                    String key = StringUtils.substringBefore(item,"\t");
                    String value = StringUtils.substringAfter(item, "\t");
                    if (StringUtils.isNotBlank(key)&& StringUtils.isNotBlank(value)
                            && validArg(value)
                            && !key.equals("uid")
                            && value.length() < 20) {
                        infoMap.put(key, value);
                    }
                }
            }
        }
        if (!infoMap.isEmpty()) {
            for (String ve : vec) {
                if (StringUtils.isNotBlank(infoMap.get(ve))) {
                    wxid = infoMap.get(ve);
                    break;
                }
            }
        }
        if (StringUtils.isBlank(wxid)) {
            if (StringUtils.isNotBlank(auid)) {
                wxid = auid;
            } else {
                wxid = addr + "#" + uagn;
            }
        }

        String daytime = tokens[0].substring(0, 19).replace("T", " ");
        String udbacID = UAHashUtils.hash(wxid);

        //生成uaid 如果ua_hash中存在 则直接取
        String uaid = null;
        String parsedUA =null;
        if (ua_hash.containsKey(uagn)) {
            uaid = ua_hash.get(uagn);
        } else {
            parsedUA = UAHashUtils.parseUA(uagn);
            uaid = UAHashUtils.hash(parsedUA);
            ua_hash.put(uagn, uaid);
        }

        return udbacID + "\t" + daytime + "\t" + uaid ;
    }
}

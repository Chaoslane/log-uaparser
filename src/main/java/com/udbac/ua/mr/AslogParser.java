package com.udbac.ua.mr;

import com.udbac.ua.util.UAHashUtils;
import com.udbac.ua.util.UnsupportedlogException;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by 43890 on 2017/4/10.
 */
public class AslogParser {

    private static Map<String, String> ua_hash = new HashMap<>(1024 * 1024);

    private static String[] array = new String[]{
            "m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
            "m1", "m3", "m1b", "m9c"};
    private static List<String> vec = new ArrayList<>(Arrays.asList(array));

    private static boolean validArg(String str) {
        String regex = "[A-Za-z0-9.-]*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }

    public static Map<String,String> asLogParser(String line) throws UnsupportedlogException {
        Map<String, String> fieldsMap = new HashMap<>();
        String[] tokens = StringUtils.splitPreserveAllTokens(line, "\t");
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
        String adop = null;
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
            if (StringUtils.isNotBlank(aurl)) {
                String[] adop_adid = aurl.split("[,&]", -1);
                adid = adop_adid[1];
                switch (adop_adid[0]) {
                    case "/c":
                        adop = "clk";
                        break;
                    case "/i":
                    case "/t":
                        adop = "imp";
                        break;
                    default:
                        throw new UnsupportedlogException(
                                "Unsupported log format Exception :" + tokens.length + " fields, bad operator :" + adop_adid[0]);
                }
            } else {
                throw new UnsupportedlogException("Unsupported log format, fetch AD operator failed.");
            }
        } else if (len == 11) {
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
            auid = tokens[10];
            if (StringUtils.isNotBlank(aurl)) {
                String[] adop_adid = aurl.split("[,&]", -1);
                switch (adop_adid[0]) {
                    case "/c":
                        adop = "clk";
                        adid = adop_adid[1];
                        break;
                    case "/i":
                        adop = "imp";
                        adid = adop_adid[1];
                        break;
                    case "/m":
                        adop = "clk";
                        adid = tokens[4];
                        break;
                    case "/s":
                        adop = "clk";
                        adid = tokens[4];
                        break;
                    case "/do":
                        adop = "";
                        adid = "";
                        break;
                    default:
                        throw new UnsupportedlogException(
                                "Unsupported log format Exception :" + tokens.length + " fields, bad operator :" + adop_adid[0]);
                }
            }
        } else if (len == 12) {
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
            auid = tokens[10];
            refr = tokens[11];
            if (aurl.contains("/s")||aurl.contains("/s")) {
                adop = "imp";
            } else if (aurl.contains("/m") || aurl.contains("/c")) {
                adop = "clk";
            } else {
                adop = "";
            }
        } else {
            throw new UnsupportedlogException("Unsupported log format, found " + tokens.length + " fields, AS log support 10/11/12 fields only.");
        }

        if (StringUtils.isBlank(adop) || StringUtils.isBlank(adid)
                || adid.length() > 24) {
            throw new UnsupportedlogException("Unsupported log format, got null adop or adid ");
        }

        //获取wxid
        String wxid = null;
        Map<String, String> infoMap = new HashMap<>();
        for (String ustr : new String[]{aurl, aarg}) {
            if (StringUtils.isNotBlank(ustr)) {
                String[] items = ustr.split("[,&]", -1);
                for (String item : items) {
                    String key = StringUtils.substringBefore(item, "=");
                    String value = StringUtils.substringAfter(item, "=");
                    if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)
                            && value.length() > 0
                            && validArg(value)
                            && !key.equals("uid")) {
                        infoMap.put(key, value.replaceAll("[^A-Za-z0-9.-]*", ""));
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

        //生成uaid 如果ua_hash中存在 则直接取
        String uaid = null;
        if (ua_hash.containsKey(uagn)) {
            uaid = ua_hash.get(uagn);
        } else {
            String parsedUA = UAHashUtils.parseUA(uagn);
            uaid = UAHashUtils.hash(parsedUA);
            ua_hash.put(uagn, uaid);
        }

        fieldsMap.put("datetime", time.substring(0, 19).replace("T", " "));
        fieldsMap.put("wxid", UAHashUtils.hash(wxid));
        fieldsMap.put("uaid", uaid);
        fieldsMap.put("adid", adid);
        fieldsMap.put("adop", adop);
        return fieldsMap;
    }
}

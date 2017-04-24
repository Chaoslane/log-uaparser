package com.udbac.ua.mr;

import com.udbac.ua.entity.AslogRaw;
import com.udbac.ua.util.UAHashUtils;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by 43890 on 2017/4/10.
 */
public class AslogHandler {

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

    static String getWxid(AslogRaw aslog) {
        String wxid = null;
        Map<String, String> infoMap = getAurlAarg(aslog);
        if (!infoMap.isEmpty()) {
            for (String ve : vec) {
                if (StringUtils.isNotBlank(infoMap.get(ve))) {
                    wxid = infoMap.get(ve);
                    break;
                }
            }
        }
        if (StringUtils.isBlank(wxid)) {
            if (StringUtils.isNotBlank(aslog.getAuid())) {
                wxid = aslog.getAuid();
            } else {
                wxid = aslog.getAddr() + "#" + aslog.getUagn();
            }
        }
        return wxid;
    }

    private static Map<String, String> getAurlAarg(AslogRaw aslog) {
        Map<String, String> infoMap = new HashMap<>();
        for (String ustr : new String[]{aslog.getAurl(), aslog.getAarg()}) {
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
        return infoMap;
    }
}

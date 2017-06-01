package com.udbac.ua.mr;

import com.udbac.ua.entity.Aslog;
import org.apache.commons.lang.StringUtils;

import java.util.*;

/**
 * Created by 43890 on 2017/4/10.
 */
public class AslogHandler {

    // 生成UID，UID逻辑：
    // 1、使用IDFA
    // 2、使用AndroidID、AndroidIDmd5
    // 3、使用MACmd5、MACBmd5、OpenUDID
    // 4、使用其余非空字段生成MD5
    private static String[] array = new String[]{
            "m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
            "m1", "m3", "m1b", "m9c"};

    public static String getWxid(Aslog aslog) {
        String wxid = null;
        Map<String, String> infoMap = aslog.getInfoMap();
        if (!infoMap.isEmpty()) {
            for (String ve : array) {
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
}

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

    private static boolean validArg(String str) {
        String regex = "[A-Za-z0-9.-]*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }

    // 对于移动端代码，优先选用宏参数定义的IP地址、用户ID
    // mo	OS			操作系统 0=Android、1=iOS、2=WP、3=Others
    // ns	IP			IP
    // m1	IMEI		md5(IMEI)
    // m2	IDFA		iOS_IDFA
    // m3	DUID		md5(WP_DUID)
    // m1a	ANDROIDID	md5(AndroidID)
    // m2a	OPENUDID	OpenUDID
    // m9	MAC1		md5(eth0_mac)，去除:
    // m9b	MAC			md5(eth0_mac)，保留:
    // m1b	AAID		Android Advertising ID
    // m1c	ANDROIDID1	AndroidID
    // m9c	ODIN		ODIN
    // nx	[UUID]		Unique User ID
    // uid	wb_uid_md5	Weibo User ID
    private static Map<String, String> handleAurlAarg(AslogRaw aslog) {
        Map<String, String> infoMap = new HashMap<>();
        for (String ustr : new String[]{aslog.getAurl(), aslog.getAarg()}) {
            if (StringUtils.isNotBlank(ustr)) {
                String[] items = ustr.split("[,&]", -1);
                for (String item : items) {
                    String key = StringUtils.substringBefore(item, "=");
                    String value = StringUtils.substringAfter(item, "=");
                    // 对于所有MMA宏参数而言，其值的取值范围应该是固定的，应该滤除非法字符
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

    // 生成UID，UID逻辑：
    // 1、使用IDFA
    // 2、使用AndroidID、AndroidIDmd5
    // 3、使用MACmd5、MACBmd5、OpenUDID
    // 4、使用其余非空字段生成MD5
    private static String[] array = new String[]{
            "m2", "m1c", "m1a", "m9b", "m9", "m2a", "uid",
            "m1", "m3", "m1b", "m9c"};
    private static List<String> vec = new ArrayList<>(Arrays.asList(array));

    static String getWxid(AslogRaw aslog) {
        String wxid = null;
        Map<String, String> infoMap = handleAurlAarg(aslog);
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
}

package com.udbac.ua.util;

import com.udbac.ua.entity.UAinfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;

import javax.script.ScriptException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by root on 2017/2/14.
 */
public class AsUtils {

    public static String hash(String uaString) throws IllegalArgumentException {
        byte[] sha1ed = DigestUtils.sha(uaString);
        String base64ed = Base64.encodeBase64String(sha1ed);
        // 转为URL安全编码
        String safeurl = base64ed.replace("+", "-")
                .replace("/", "_")
                .replace("=", "")
                // 转为定长且只有字母数字的编码
                .replaceAll("[-_]", "");

        if (safeurl.length() < 20) {
            throw new IllegalArgumentException("Got hashid too short:" + safeurl);
            // base64算法的结尾为 \r\n 去掉
        } else return safeurl.substring(0, 20).replaceAll("[\r\n]", "");
    }


    public static String urlDecode(String strUrl) {
        try {
            // 转换C风格\x为%  转换多个%2525为%
            strUrl = strUrl.replace("\\x", "%").replace("%25", "%");
            return URLDecoder.decode(strUrl, "UTF-8");
        } catch (UnsupportedEncodingException | IllegalArgumentException e) {
            return "";
        }
    }


}

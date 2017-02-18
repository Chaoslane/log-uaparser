package com.udbac.ua.mr;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;

/**
 * Created by root on 2017/2/14.
 */
public class UAHashUtils {
    private static Parser uapaser;
    static {
        try {
            uapaser = new Parser();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected static String hashUA(String uaString) {
        byte[] sha1ed = DigestUtils.sha(uaString);
        String base64ed = Base64.encodeBase64String(sha1ed);
        return base64ed.replaceAll("[/+_-]","").substring(0, 20);
    }

    protected static String handleUA(String uaString) {
        Client c = uapaser.parse(uaString);
        return    c.os.family + "\t"
                + (StringUtils.isNotBlank(c.os.major) && StringUtils.isNotBlank(c.os.minor) ? c.os.major + "." + c.os.minor : "") + "\t"
                + c.userAgent.family + "\t"
                + (StringUtils.isNotBlank(c.userAgent.major) && StringUtils.isNotBlank(c.userAgent.minor) ? c.userAgent.major + "." + c.userAgent.minor : "") + "\t"
                + c.device.family + "\t"
                + (StringUtils.isNotBlank(c.device.brand)? c.device.brand:"") + "\t"
                + (StringUtils.isNotBlank(c.device.model)?c.device.model:"");
    }

    public enum MyCounters {
        ALLLINECOUNTER
    }
}

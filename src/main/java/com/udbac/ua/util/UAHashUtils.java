package com.udbac.ua.util;

import com.udbac.ua.entity.UAinfo;
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

    public static String hash(String uaString) {
        byte[] sha1ed = DigestUtils.sha(uaString);
        String base64ed = Base64.encodeBase64String(sha1ed);
        return base64ed.replaceAll("[/+_-]","").substring(0, 20);
    }

    public static String parseUA(String uaStr) {
        String[] vs = uaStr.split("[^A-Za-z0-9_-]");
        UAinfo uAinfo = new UAinfo();
        if (vs.length > 0) {
            //优酷
            switch (vs[0]) {
                case "Youku":
                case "Tudou": {
                    uAinfo = othYouku(uaStr);
                    String[] vs1 = uaStr.split("[;]");
                    if (vs1.length == 5) {
                        uAinfo.setCatalog("Youku");
                        uAinfo.setBrowser(vs1[0]);
                        uAinfo.setBrowserver(vs1[1]);
                        uAinfo.setOs(vs1[2]);
                        uAinfo.setOsver(vs1[3]);
                        uAinfo.setDevice(vs1[4]);
                        //model 1...50
                        if (StringUtils.isNotBlank(vs1[4]) && vs1[4].length() < 50) {
                            uAinfo.setModel(vs1[4]);
                        }
                    } else if (vs1.length == 4) {
                        String[] vs2 = uaStr.split("[; /]");
                        if (vs2.length == 6) {
                            uAinfo.setCatalog("Youku");
                            uAinfo.setBrowser(vs2[0]);
                            uAinfo.setBrowserver(vs2[2] + "/" + vs2[3] + " " + vs2[1]);
                            uAinfo.setOs(vs2[4]);
                            uAinfo.setOsver(vs2[5]);
                        } else {
                            uAinfo = othYouku(uaStr);
                        }
                    } else {
                        uAinfo = othYouku(uaStr);
                    }
                    break;
                }
                case "QYPlayer":
                case "Cupid": {
                    uAinfo = othIqiyi(uaStr);
                    String[] vs1 = uaStr.split("[;/]");
                    if (vs1.length == 2) {
                        uAinfo.setCatalog("iQiyi");
                        uAinfo.setBrowser(vs1[0]);
                        uAinfo.setBrowserver(vs1[1]);
                    } else if (vs1.length == 3) {
                        uAinfo.setCatalog("iQiyi");
                        uAinfo.setBrowser(vs1[0]);
                        uAinfo.setBrowserver(vs1[2]);
                        uAinfo.setOs(vs1[1]);
                    } else {
                        uAinfo = othIqiyi(uaStr);
                    }
                    break;
                }
                default:
                    //使用uap转换
                    Client c = uapaser.parse(uaStr);
                    if ("Other".equals(c.userAgent.family)) {
                        uAinfo.setCatalog("Other");
                    } else {
                        uAinfo.setCatalog("Normal");
                    }
                    uAinfo.setBrowser(c.userAgent.family);
                    uAinfo.setBrowserver(getOrElse(c.userAgent.major)
                            + getOrElse(c.userAgent.minor, ".") + getOrElse(c.userAgent.patch, "."));
                    uAinfo.setOs(c.os.family);
                    uAinfo.setOsver(getOrElse(c.os.major)
                            + getOrElse(c.os.minor, ".") + getOrElse(c.os.patch, "."));
                    uAinfo.setDevice(c.device.family);
                    uAinfo.setBrand(getOrElse(c.device.brand));
                    uAinfo.setModel(getOrElse(c.device.model));
                    break;
            }
        }
        return uAinfo.toString();
    }

    private static String getOrElse(String string) {
        return getOrElse(string, "");
    }
    //版本号如果为null 则返回"" 有值则返回 .x
    private static String getOrElse(String string,String seprator) {
        if (StringUtils.isNotBlank(string)) {
            return seprator+string;
        }else {
            return "";
        }
    }

    private static UAinfo othYouku(String uaStr) {
        UAinfo othYouku = new UAinfo();
        othYouku.setCatalog("Youku_Other");
        othYouku.setBrowser(uaStr);
        othYouku.setOs("Other");
        othYouku.setDevice("Other");
        return othYouku;
    }

    private static UAinfo othIqiyi(String uaStr) {
        UAinfo othIqiyi = new UAinfo();
        othIqiyi.setCatalog("iQiyi_Other");
        othIqiyi.setBrowser(uaStr);
        othIqiyi.setOs("Other");
        othIqiyi.setDevice("Other");
        return othIqiyi;
    }

    public enum MyCounters {
        ALLLINECOUNTER
    }
}
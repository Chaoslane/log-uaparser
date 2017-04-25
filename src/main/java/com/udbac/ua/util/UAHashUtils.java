package com.udbac.ua.util;

import com.udbac.ua.entity.UAinfo;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import ua_parser.Client;
import ua_parser.Parser;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * Created by root on 2017/2/14.
 */
public class UAHashUtils {
    private static Parser uapaser;

    static {
        try {
            uapaser = new Parser();
        } catch (IOException e) {
            throw new RuntimeException("Load ua parser failed");
        }
    }

    public static String hash(String uaString) throws HashIdException {
        byte[] sha1ed = DigestUtils.sha(uaString);
        String base64ed = Base64.encodeBase64String(sha1ed);
        // 转为URL安全编码
        String safeurl = base64ed.replace("+", "-")
                .replace("/", "_")
                .replace("=", "")
                // 转为定长且只有字母数字的编码
                .replaceAll("[-_]", "");
        if (safeurl.length() < 20) {
            throw new HashIdException("Got hashid too short:" + safeurl);
            // base64算法的结尾为 \r\n 去掉
        } else return safeurl.substring(0, 20).replaceAll("[\r\n]", "");
    }

    public static String parseUA(String uaStr) {
        String[] vs = uaStr.split("[^A-Za-z0-9_-]", -1);
        UAinfo uAinfo = new UAinfo();
        // 爱奇艺、优酷使用自有的UAinfo串，因此需要单独识别
        if (vs.length > 0) {
            //优酷
            switch (vs[0]) {
                case "Youku":
                case "Tudou": {
                    uAinfo = othYouku(uaStr);
                    String[] vs1 = uaStr.split("[;]", -1);
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
                        String[] vs2 = uaStr.split("[; /]", -1);
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
                    String[] vs1 = uaStr.split("[;/]", -1);
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
                    // 利用parser(https://github.com/ua-parser)组件，解析UA串
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

        String uaInfoStr = uAinfo.toString();
        //判断是否含有urlencode编码串，进行解析
        uaInfoStr = uaInfoStr.matches(".*(\\\\x[A-Za-z0-9]{2})+.*|.*(%[A-Za-z0-9]{2})+.*")
                ? urlDecode(uaInfoStr) : uaInfoStr;

        if (StringUtils.isNotBlank(uaInfoStr)) {
            // 去掉导致入库失败的特殊字符
            uaInfoStr = uaInfoStr.replace("+", " ").replaceAll("(\\\\x[A-Za-z0-9]{0,2})|\\\\|R%H|R%|%", "");
            // 验证uainfo是否可入库
            String[] infos = uaInfoStr.split("\t", -1);
            if (infos.length == 8) {
                boolean flag = true;
                for (String info : infos) {
                    if (info.length() > 50) {
                        flag = false;
                        break;
                    }
                }
                if (flag) {
                    return uaInfoStr;
                }
            }
        }
        return new UAinfo().toString();
    }

    private static String urlDecode(String strUrl) {
        try {
            // 转换C风格\x为%  转换多个%2525为%
            strUrl = strUrl.replace("\\x", "%").replace("%25", "%");
            return URLDecoder.decode(strUrl, "UTF-8");
        } catch (UnsupportedEncodingException | IllegalArgumentException e) {
            return "";
        }
    }

    private static String getOrElse(String string) {
        return getOrElse(string, "");
    }

    //版本号如果为null 则返回"" 有值则返回 .x
    private static String getOrElse(String string, String seprator) {
        if (StringUtils.isNotBlank(string)) {
            return seprator + string;
        } else {
            return "";
        }
    }

    // 默认优酷
    private static UAinfo othYouku(String uaStr) {
        UAinfo othYouku = new UAinfo();
        othYouku.setCatalog("Youku_Other");
        othYouku.setBrowser(uaStr);
        othYouku.setOs("Other");
        othYouku.setDevice("Other");
        return othYouku;
    }

    //默认爱奇艺
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

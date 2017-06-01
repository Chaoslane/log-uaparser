package com.udbac.ua.entity;

import com.udbac.ua.uap.CachingParser;
import com.udbac.ua.uap.Client;
import com.udbac.ua.uap.Parser;
import org.apache.commons.lang.StringUtils;

/**
 * Created by root on 2017/2/20.
 * UAinfo 实体类
 */
public class UAinfo {
    private String catalog;
    private String browser;
    private String browserver;
    private String os;
    private String osver;
    private String device;
    private String brand;
    private String model;

    public UAinfo() {}

    public UAinfo(String catalog, String browser, String browserver, String os, String osver, String device, String brand, String model) {
        this.catalog = catalog;
        this.browser = browser;
        this.browserver = browserver;
        this.os = os;
        this.osver = osver;
        this.device = device;
        this.brand = brand;
        this.model = model;
    }


    public String parseUagn2Str(String uaStr) {
        return parseUagn(uaStr).toString();
    }

    public UAinfo parseUagn(String uaStr){
        UAinfo uAinfo;
        if (StringUtils.isBlank(uaStr)) {
            return other();
        }else uAinfo = new UAinfo();

        //去除urldecode编码
        uaStr = uaStr.replaceAll("(\\\\x[A-Za-z0-9]{2}+)|(%[A-Za-z0-9]{2})+", "");

        String[] vs = uaStr.split("[^A-Za-z0-9_-]", -1);
        // 爱奇艺、优酷使用自有的UAinfo串，因此需要单独识别
        if (vs.length > 0) {
            //优酷
            switch (vs[0]) {
                case "Youku":
                case "Tudou": {
                    String[] vs1 = uaStr.split("[;]", -1);
                    if (vs1.length == 5) {
                        uAinfo.catalog = "Youku";
                        uAinfo.browser = vs1[0];
                        uAinfo.browserver = vs1[1];
                        uAinfo.os = vs1[2];
                        uAinfo.osver = vs1[3];
                        uAinfo.device = replaceIllegal(vs1[4]);
                        uAinfo.brand = "";
                        uAinfo.model = "";
                        //model 1...50
                        if (StringUtils.isNotBlank(vs1[4]) && vs1[4].length() < 50) {
                            uAinfo.model =replaceIllegal(vs1[4]);
                        }
                    } else if (vs1.length == 4) {
                        String[] vs2 = uaStr.split("[ ;/]", -1);
                        if (vs2.length == 6) {
                            uAinfo.catalog = "Youku";
                            uAinfo.browser = vs2[0];
                            uAinfo.browserver = vs2[2] + "/" + vs2[3] + " " + vs2[1];
                            uAinfo.os = vs2[4];
                            uAinfo.osver = vs2[5];
                            uAinfo.device = "";
                            uAinfo.brand = "";
                            uAinfo.model = "";
                        }
                    } else uAinfo = othYouku(uaStr);
                    break;
                }
                case "QYPlayer":
                case "Cupid": {
                    String[] vs1 = uaStr.split("[;/]", -1);
                    if (vs1.length == 2) {
                        uAinfo.catalog = "iQiyi";
                        uAinfo.browser = vs1[0];
                        uAinfo.browserver = vs1[1];
                        uAinfo.os = "";
                        uAinfo.osver = "";
                        uAinfo.device = "";
                        uAinfo.brand = "";
                        uAinfo.model = "";
                    } else if (vs1.length == 3) {
                        uAinfo.catalog = "iQiyi";
                        uAinfo.browser = vs1[0];
                        uAinfo.browserver = vs1[2];
                        uAinfo.os = vs1[1];
                        uAinfo.osver = "";
                        uAinfo.device = "";
                        uAinfo.brand = "";
                        uAinfo.model = "";
                    } else uAinfo = othIqiyi(uaStr);
                    break;
                }
                default:
                    uAinfo = uaParser(uaStr);
                    break;
            }
        }
        return uAinfo;
    }

    private UAinfo uaParser(String uaStr) {
        Parser parser = CachingParser.getInstance();
        Client c = parser.parse(uaStr);

        UAinfo uAinfo = new UAinfo();
        if (null == c) return other();
        uAinfo.catalog = "Other".equals(c.userAgent.family) ? "Other" : "Normal";
        uAinfo.browser = StringUtils.trimToEmpty(c.userAgent.family);

        uAinfo.browserver = buildVersion(c.userAgent.major, c.userAgent.minor, c.userAgent.patch);
        uAinfo.os = StringUtils.trimToEmpty(c.os.family);
        uAinfo.osver = buildVersion(c.os.major, c.os.minor, c.os.patch);

        uAinfo.device = replaceIllegal(c.device.family);
        uAinfo.brand = replaceIllegal(c.device.brand);
        uAinfo.model = replaceIllegal(c.device.model);
        return uAinfo;
    }

    private static String replaceIllegal(String str) {
        if (null != str) {
            str = StringUtils.trimToEmpty(
                    str.replaceAll("[?;+%\\\\]", ""));
        } else str = "";
        return str;
    }

    private static String buildVersion(String major, String minor, String patch) {
        if (StringUtils.isBlank(major)) {
            return "";
        }
        if (StringUtils.isBlank(minor)) {
            return major;
        } else if (StringUtils.isBlank(patch)) {
            return major + "." + minor;
        } else {
            return major + "." + minor + "." + patch;
        }
    }

    // 默认
    private static UAinfo other() {
        return new UAinfo("Other", "Other",
                "", "Other", "", "Other", "", "");
    }

    // 默认优酷
    private static UAinfo othYouku(String uaStr) {
        return new UAinfo("Youku_Other", uaStr, "", "Other", "", "Other", "", "");
    }

    //默认爱奇艺
    private static UAinfo othIqiyi(String uaStr) {
        return new UAinfo("iQiyi_Other", uaStr, "", "Other", "", "Other", "", "");
    }

    @Override
    public String toString() {
        return catalog + '\t' + browser + '\t' + browserver + '\t' +
                os + '\t' + osver + '\t' +
                device + '\t' + brand + '\t' + model;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getBrowser() {
        return browser;
    }

    public String getBrowserver() {
        return browserver;
    }

    public String getOs() {
        return os;
    }

    public String getOsver() {
        return osver;
    }

    public String getDevice() {
        return device;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }
}

package com.udbac.ua.entity;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by chaoslane on 2017/4/24.
 * as日志实体类 原始字段
 */
public class AslogRaw {
    private static final Logger logger = Logger.getLogger(AslogRaw.class);

    /**
     * 一行日志->原始的aslog实体，未解析各tokens
     * 有效的as日志必须有合法的 adid adop，处理后仍不合法，则丢掉
     * @param line 一行日志
     * @return AslogRaw
     */
    public static AslogRaw parseAslog(String line) {
        if (StringUtils.isBlank(line)) return null;
        String[] tokens = line.split("\t", -1);
        int len = tokens.length;
        AslogRaw as;
        if (len == 10) {
            as = new AslogRaw();
            as.time = tokens[0];
            as.msec = tokens[1];
            as.addr = tokens[2];
            as.xfwd = tokens[3];
            as.adid = tokens[4];  //ADID有问题，均为空
            as.aurl = tokens[5];
            as.aarg = tokens[6];
            as.areq = tokens[7];
            as.uagn = tokens[8];
            as.ckie = tokens[9];
            if (StringUtils.isNotBlank(as.aurl)) {
                String[] adop_adid = as.aurl.split("[,&]", -1);
                as.adid = adop_adid[1];
                switch (adop_adid[0]) {
                    case "/c":
                        as.adop = "clk";
                        break;
                    case "/i":
                    case "/t":
                        as.adop = "imp";
                        break;
                    default:
                        logger.warn("Unsupported log format Exception : bad operator :" + adop_adid[0]);
                        return null;
                }
            } else {
                logger.warn("Unsupported log format, fetch AD operator failed.");
                return null;
            }
        } else if (len == 11) {
            as = new AslogRaw();
            as.time = tokens[0];
            as.msec = tokens[1];
            as.addr = tokens[2];
            as.xfwd = tokens[3];
            as.adid = tokens[4];
            as.aurl = tokens[5];
            as.aarg = tokens[6];
            as.areq = tokens[7];
            as.uagn = tokens[8];
            as.ckie = tokens[9];
            as.auid = tokens[10];
            if (StringUtils.isNotBlank(as.aurl)) {
                String[] adop_adid = as.aurl.split("[,&]", -1);
                switch (adop_adid[0]) {
                    case "/c":
                        as.adop = "clk";
                        as.adid = adop_adid[1];
                        break;
                    case "/i":
                        as.adop = "imp";
                        as.adid = adop_adid[1];
                        break;
                    case "/m":
                        as.adop = "clk";
                        as.adid = tokens[4];
                        break;
                    case "/s":
                        as.adop = "clk";
                        as.adid = tokens[4];
                        break;
                    case "/do":
                        as.adop = "";
                        as.adid = "";
                        break;
                    default:
                        logger.warn("Unsupported log format Exception, bad operator :" + adop_adid[0]);
                        return null;
                }
            }
        } else if (len == 12) {
            as = new AslogRaw();
            as.time = tokens[0];
            as.msec = tokens[1];
            as.addr = tokens[2];
            as.xfwd = tokens[3];
            as.adid = tokens[4];
            as.aurl = tokens[5];
            as.aarg = tokens[6];
            as.areq = tokens[7];
            as.uagn = tokens[8];
            as.ckie = tokens[9];
            as.auid = tokens[10];
            as.refr = tokens[11];
            if (as.aurl.contains("/s") || as.aurl.contains("/s")) {
                as.adop = "imp";
            } else if (as.aurl.contains("/m") || as.aurl.contains("/c")) {
                as.adop = "clk";
            } else {
                as.adop = "";
            }
        } else {
            logger.warn("Unsupported log format, found " + tokens.length + " fields, AS log support 10/11/12 fields only.");
            return null;
        }

        as.time = as.time.substring(0, 19).replace("T", " ");
        as.adid = as.adid.replaceAll("\\\\x|\\\\", "");

        if (StringUtils.isBlank(as.adop)
                || StringUtils.isBlank(as.adid) || as.adid.length() > 24) {
            logger.warn("Got Illegal adop or adid ");
            return null;
        }
        return as;
    }


    private String time;
    private String msec;
    private String addr;
    private String xfwd;
    private String adid; // ADID有问题，均为空
    private String aurl;
    private String aarg;
    private String areq;
    private String uagn;
    private String ckie;
    private String auid;
    private String refr;
    private String adop;

    public String getTime() {
        return time;
    }

    public String getMsec() {
        return msec;
    }

    public String getAddr() {
        return addr;
    }

    public String getXfwd() {
        return xfwd;
    }

    public String getAdid() {
        return adid;
    }

    public String getAurl() {
        return aurl;
    }

    public String getAarg() {
        return aarg;
    }

    public String getAreq() {
        return areq;
    }

    public String getUagn() {
        return uagn;
    }

    public String getCkie() {
        return ckie;
    }

    public String getAuid() {
        return auid;
    }

    public String getRefr() {
        return refr;
    }

    public String getAdop() {
        return adop;
    }
}

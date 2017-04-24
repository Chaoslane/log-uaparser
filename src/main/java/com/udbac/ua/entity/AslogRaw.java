package com.udbac.ua.entity;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by root on 2017/4/24.
 */
public class AslogRaw {
    private static final Logger logger = Logger.getLogger(AslogRaw.class);

    public static AslogRaw parseAslog(String line) {
        if (StringUtils.isBlank(line)) {
            return null;
        }
        String[] tokens = line.split("\t", -1);
        int len = tokens.length;
        AslogRaw aslog = null;
        if (len == 10) {
            aslog = new AslogRaw();
            aslog.time = tokens[0];
            aslog.msec = tokens[1];
            aslog.addr = tokens[2];
            aslog.xfwd = tokens[3];
            aslog.adid = tokens[4];
            aslog.aurl = tokens[5];
            aslog.aarg = tokens[6];
            aslog.areq = tokens[7];
            aslog.uagn = tokens[8];
            aslog.ckie = tokens[9];
            if (StringUtils.isNotBlank(aslog.aurl)) {
                String[] adop_adid = aslog.aurl.split("[,&]", -1);
                aslog.adid = adop_adid[1];
                switch (adop_adid[0]) {
                    case "/c":
                        aslog.adop = "clk";
                        break;
                    case "/i":
                    case "/t":
                        aslog.adop = "imp";
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
            aslog = new AslogRaw();
            aslog.time = tokens[0];
            aslog.msec = tokens[1];
            aslog.addr = tokens[2];
            aslog.xfwd = tokens[3];
            aslog.adid = tokens[4];
            aslog.aurl = tokens[5];
            aslog.aarg = tokens[6];
            aslog.areq = tokens[7];
            aslog.uagn = tokens[8];
            aslog.ckie = tokens[9];
            aslog.auid = tokens[10];
            if (StringUtils.isNotBlank(aslog.aurl)) {
                String[] adop_adid = aslog.aurl.split("[,&]", -1);
                switch (adop_adid[0]) {
                    case "/c":
                        aslog.adop = "clk";
                        aslog.adid = adop_adid[1];
                        break;
                    case "/i":
                        aslog.adop = "imp";
                        aslog.adid = adop_adid[1];
                        break;
                    case "/m":
                        aslog.adop = "clk";
                        aslog.adid = tokens[4];
                        break;
                    case "/s":
                        aslog.adop = "clk";
                        aslog.adid = tokens[4];
                        break;
                    case "/do":
                        aslog.adop = "";
                        aslog.adid = "";
                        break;
                    default:
                        logger.warn("Unsupported log format Exception, bad operator :" + adop_adid[0]);
                        return null;
                }
            }
        } else if (len == 12) {
            aslog = new AslogRaw();
            aslog.time = tokens[0];
            aslog.msec = tokens[1];
            aslog.addr = tokens[2];
            aslog.xfwd = tokens[3];
            aslog.adid = tokens[4];
            aslog.aurl = tokens[5];
            aslog.aarg = tokens[6];
            aslog.areq = tokens[7];
            aslog.uagn = tokens[8];
            aslog.ckie = tokens[9];
            aslog.auid = tokens[10];
            aslog.refr = tokens[11];
            if (aslog.aurl.contains("/s") || aslog.aurl.contains("/s")) {
                aslog.adop = "imp";
            } else if (aslog.aurl.contains("/m") || aslog.aurl.contains("/c")) {
                aslog.adop = "clk";
            } else {
                aslog.adop = "";
            }
        } else {
            logger.warn("Unsupported log format, found " + tokens.length + " fields, AS log support 10/11/12 fields only.");
            return null;
        }

        aslog.time = aslog.time.substring(0, 19).replace("T", " ");
        aslog.adid = aslog.adid.replaceAll("\\\\x|\\\\", "");

        if (StringUtils.isBlank(aslog.adop)
                || StringUtils.isBlank(aslog.adid) || aslog.adid.length() > 24) {
            logger.warn("Got Illegal adop or adid ");
            return null;
        }
        return aslog;
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

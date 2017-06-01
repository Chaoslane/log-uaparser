package com.udbac.ua.entity;

import com.udbac.ua.util.LogParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by chaoslane on 2017/4/24.
 * as日志实体类 原始字段
 */
public class Aslog {
    private static final Logger logger = Logger.getLogger(Aslog.class);

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
    private Map<String,String> infoMap;

    public Aslog() {}

    /**
     * 一行日志->原始的aslog实体，未解析各tokens
     * 有效的as日志必须有合法的 adid adop，处理后仍不合法，则丢掉
     * @return Aslog
     */
    public Aslog(String line) throws LogParseException {
        parseAslog(line);
    }

    private void parseAslog(String line) throws LogParseException {
        String[] tokens = line.split("\t", -1);
        int len = tokens.length;
        if (len == 10) {
            this.time = tokens[0];
            this.msec = tokens[1];
            this.addr = tokens[2];
            this.xfwd = tokens[3];
            this.adid = tokens[4];  //ADID有问题，均为空
            this.aurl = tokens[5];
            this.aarg = tokens[6];
            this.areq = tokens[7];
            this.uagn = tokens[8];
            this.ckie = tokens[9];
            if (StringUtils.isNotBlank(this.aurl)) {
                String[] adop_adid = this.aurl.split("[,&]", -1);
                this.adid = adop_adid[1];
                switch (adop_adid[0]) {
                    case "/c":
                        this.adop = "clk";
                        break;
                    case "/i":
                    case "/t":
                        this.adop = "imp";
                        break;
                    default:
                        throw new LogParseException("Unsupported log format Exception : bad operator :" + adop_adid[0]);
                }
            } else {
                throw new LogParseException("Unsupported log format, fetch AD operator failed.");
            }
        } else if (len == 11) {
            this.time = tokens[0];
            this.msec = tokens[1];
            this.addr = tokens[2];
            this.xfwd = tokens[3];
            this.adid = tokens[4];
            this.aurl = tokens[5];
            this.aarg = tokens[6];
            this.areq = tokens[7];
            this.uagn = tokens[8];
            this.ckie = tokens[9];
            this.auid = tokens[10];
            if (StringUtils.isNotBlank(this.aurl)) {
                String[] adop_adid = this.aurl.split("[,&]", -1);
                switch (adop_adid[0]) {
                    case "/c":
                        this.adop = "clk";
                        this.adid = adop_adid[1];
                        break;
                    case "/i":
                        this.adop = "imp";
                        this.adid = adop_adid[1];
                        break;
                    case "/m":
                        this.adop = "clk";
                        this.adid = tokens[4];
                        break;
                    case "/s":
                        this.adop = "clk";
                        this.adid = tokens[4];
                        break;
                    case "/do":
                        this.adop = "";
                        this.adid = "";
                        break;
                    default:
                        throw new LogParseException("Unsupported log format Exception, bad operator :" + adop_adid[0]);
                }
            }
        } else if (len == 12) {
            this.time = tokens[0];
            this.msec = tokens[1];
            this.addr = tokens[2];
            this.xfwd = tokens[3];
            this.adid = tokens[4];
            this.aurl = tokens[5];
            this.aarg = tokens[6];
            this.areq = tokens[7];
            this.uagn = tokens[8];
            this.ckie = tokens[9];
            this.auid = tokens[10];
            this.refr = tokens[11];
            if (this.aurl.contains("/s") || this.aurl.contains("/s")) {
                this.adop = "imp";
            } else if (this.aurl.contains("/m") || this.aurl.contains("/c")) {
                this.adop = "clk";
            } else {
                this.adop = "";
            }
        } else {
            throw new LogParseException("Unsupported log format, found " + tokens.length + " fields, AS log support 10/11/12 fields only.");
        }

        this.time = this.time.substring(0, 19).replace("T", " ");
        this.adid = this.adid.replaceAll("\\\\x|\\\\", "");

        if (StringUtils.isBlank(this.adop)
                || StringUtils.isBlank(this.adid) || this.adid.length() > 24) {
            throw new LogParseException("Got Illegal adop or adid ");
        }

        this.infoMap = handleAurlAarg(this.aurl, this.aarg);
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
    private Map<String, String> handleAurlAarg(String aurl, String aarg) {
        Map<String, String> infoMap = new HashMap<>();
        for (String ustr : new String[]{aurl, aarg}) {
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

    private static boolean validArg(String str) {
        String regex = "[A-Za-z0-9.-]*";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        return matcher.matches();
    }


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

    public Map<String, String> getInfoMap() {
        return infoMap;
    }
}

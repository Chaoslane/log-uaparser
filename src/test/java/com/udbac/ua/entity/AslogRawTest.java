package com.udbac.ua.entity;

import static org.junit.Assert.*;

/**
 * Created by root on 2017/4/24.
 */
public class AslogRawTest {
    @org.junit.Test
    public void parseAslog() throws Exception {
        String line = "2016-12-14T04:00:02+08:00\t1481659202.927\t60.252.73.47\t-\tQnVEK7KBAAApF\\x\\whkMwca8Jb\t/s,QnVEK7KBpFwhkMwca8Jb\t&time=20161214040003\tGET /s,QnVEK7KBpFwhkMwca8Jb?&time=20161214040003 HTTP/1.0\tCMREADBC_Android_800*1217_V6.60(800*1217;Jlinksz;S960;Android 5.1;cn;);\tUXCM=CqUrklhFebIWvgTFNP6OAg==; WXID=soC2h6Jn8foPuuN8ELVX;\tsoC2h6Jn8foPuuN8ELVX\thttp://wap.cmread.com/rbc/p/zonghesy.jsp?vt=3&ftl_edition_id=7&timestamp=1481659180178&cm=M8020001";
        AslogRaw aslog = AslogRaw.parseAslog(line);
        System.out.println(aslog.getAdid());
    }

}
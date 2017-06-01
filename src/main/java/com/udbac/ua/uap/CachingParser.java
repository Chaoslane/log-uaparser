package com.udbac.ua.uap;

import com.google.common.collect.Maps;
import org.apache.commons.collections.map.LRUMap;

import java.io.InputStream;
import java.util.Map;

/**
 * When doing webanalytics (with for example PIG) the main pattern is to process
 * weblogs in clickstreams. A basic fact about common clickstreams is that in
 * general the same browser will do multiple requests in sequence. This has the
 * effect that the same useragent will appear in the logfiles and we will see
 * the need to parse the same useragent over and over again.
 * <p>
 * This class introduces a very simple LRU cache to reduce the number of times
 * the parsing is actually done.
 *
 * @author Niels Basjes
 */
public class CachingParser extends Parser {

    private static class SingleCachingParser {
        private static final CachingParser INSTANCE = new CachingParser();
    }

    public static CachingParser getInstance() {
        return SingleCachingParser.INSTANCE;
    }


    // TODO: Make configurable
    private static final int CACHE_SIZE = 204800;

    private static Map<String, Client> cacheClient = null;

    // ------------------------------------------

    public CachingParser() {
        super();
    }

    public CachingParser(InputStream regexYaml) {
        super(regexYaml);
    }

    // ------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public Client parse(String agentString) {
        if (agentString == null) {
            return null;
        }
        if (cacheClient == null) {
            cacheClient = new LRUMap(CACHE_SIZE);
        }
        Client client = cacheClient.get(agentString);
        if (client != null) {
            return client;
        }
        client = super.parse(agentString);
        cacheClient.put(agentString, client);
        return client;
    }

    // ------------------------------------------

}

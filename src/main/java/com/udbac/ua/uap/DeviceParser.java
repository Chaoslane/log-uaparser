/**
 * Copyright 2012 Twitter, Inc
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.udbac.ua.uap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Device parser using ua-parser regexes. Extracts device information from user agent strings.
 *
 * @author Steve Jiang (@sjiang) <gh at iamsteve com>
 */
public class DeviceParser {
    List<DevicePattern> patterns;

    public DeviceParser(List<DevicePattern> patterns) {
        this.patterns = patterns;
    }

    public Device parse(String agentString) {
        if (agentString == null) {
            return null;
        }

        Device device;
        for (DevicePattern p : patterns) {
            if ((device = p.match(agentString)) != null) {
                return device;
            }
        }
        return new Device("Other", null, null);
    }

    public static DeviceParser fromList(List<Map<String, String>> configList) {
        List<DevicePattern> configPatterns = new ArrayList<DevicePattern>();
        for (Map<String, String> configMap : configList) {
            configPatterns.add(DeviceParser.patternFromMap(configMap));
        }
        return new DeviceParser(configPatterns);
    }

    protected static DevicePattern patternFromMap(Map<String, String> configMap) {
        String regex = configMap.get("regex");
        if (regex == null) {
            throw new IllegalArgumentException("Device is missing regex");
        }
        Pattern pattern = "i".equals(configMap.get("regex_flag")) // no ohter flags used (by now)
                ? Pattern.compile(regex, Pattern.CASE_INSENSITIVE) : Pattern.compile(regex);
        return new DevicePattern(pattern, configMap.get("device_replacement"), configMap.get("brand_replacement"), configMap.get("model_replacement"));
    }

    protected static class DevicePattern {
        private static final Pattern SUBSTITUTIONS_PATTERN = Pattern.compile("\\$\\d");
        private final Pattern pattern;
        private final String deviceReplacement, brandReplacement, modelReplacement;

        public DevicePattern(Pattern pattern, String deviceReplacement, String brandReplacement, String modelReplacement) {
            this.pattern = pattern;
            this.deviceReplacement = deviceReplacement;
            this.brandReplacement = brandReplacement;
            this.modelReplacement = modelReplacement;
        }

        public Device match(String agentString) {
            Matcher matcher = pattern.matcher(agentString);
            if (!matcher.find()) {
                return null;
            }
            String[] replacements = {deviceReplacement, brandReplacement, modelReplacement};
            List<String> arglist = new ArrayList<String>();
            String device = null;
            for (String replace:replacements) {
                if (replace != null) {
                    if (replace.contains("$")) {
                        device = replace;
                        for (String substitution : getSubstitutions(replace)) {
                            int i = Integer.valueOf(substitution.substring(1));
                            String replacement = matcher.groupCount() >= i && matcher.group(i) != null
                                    ? Matcher.quoteReplacement(matcher.group(i)) : "";
                            device = device.replaceFirst("\\" + substitution, replacement);
                        }
                        device = device.trim();
                    } else {
                        device = replace;
                    }
                } else if (matcher.groupCount() >= 1) {
                    device = matcher.group(1);
                }
                arglist.add(device);
            }
            return new Device(arglist.get(0), arglist.get(1), arglist.get(2));
        }

        private List<String> getSubstitutions(String deviceReplacement) {
            Matcher matcher = SUBSTITUTIONS_PATTERN.matcher(deviceReplacement);
            List<String> substitutions = new ArrayList<String>();
            while (matcher.find()) {
                substitutions.add(matcher.group());
            }
            return substitutions;
        }

    }

}
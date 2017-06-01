package com.udbac.ua.util;

/**
 * Created by root on 2016/7/13.
 */
public class SplitValueBuilder {
    private String split = "\t";
    private StringBuilder sb = new StringBuilder();

    public SplitValueBuilder() {
    }

    public SplitValueBuilder(String split) {
        this.split = split;
    }

    public SplitValueBuilder add(String value) {
        this.sb.append(value).append(split);
        return this;
    }

    public SplitValueBuilder add(int value) {
        this.sb.append(String.valueOf(value)).append(split);
        return this;
    }

    public SplitValueBuilder add(long value) {
        this.sb.append(String.valueOf(value)).append(split);
        return this;
    }

    public SplitValueBuilder add(Object value) {
        this.sb.append(String.valueOf(value)).append(split);
        return this;
    }

    public String buildWithLast() {
        return this.sb.toString();
    }

    public String build() {
        return toString();
    }

    public String toString() {
        int index = this.sb.lastIndexOf(split);
        if (index > 0 && (index + split.length()) == this.sb.length()) {
            return this.sb.substring(0, index);
        }
        return this.sb.toString();
    }
}

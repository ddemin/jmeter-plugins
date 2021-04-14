package org.apache.jmeter.visualizers.backend.influxdb2;

import java.time.Instant;
import java.util.Map;

public class LineProtocolMessageBuilder {

    private static final char CHAR_UNIX_NEW_LINE = '\n';

    private final StringBuilder stringBuilder = new StringBuilder();

    private boolean fieldAdded = false;
    private int addedLines = 0;

    public String build() {
        return stringBuilder.toString();
    }

    public int getAddedLines() {
        return addedLines;
    }

    public LineProtocolMessageBuilder appendTags(Map<String, String> tags) {
        tags.forEach(this::appendLineProtocolTag);
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolMeasurement(String measurement) {
        return appendLineProtocolRawData(
                measurement
                        .replace(" ", "\\ ")
                        .replace(",", "\\,")
                        .replace("\n", "\\ ")
        );
    }

    public LineProtocolMessageBuilder appendLineProtocolTag(String key, String value) {
        appendLineProtocolItemDelimiter();
        appendLineProtocolRawData(
                key
                        .replace(" ", "\\ ")
                        .replace(",", "\\,")
                        .replace("=", "\\=")
        );
        appendKeyValueDelimiter();
        return appendLineProtocolRawData(
                value
                        .replace(" ", "\\ ")
                        .replace(",", "\\,")
                        .replace("=", "\\=")
                        .replace("\n", "\\ ")
        );
    }

    public LineProtocolMessageBuilder appendLineProtocolField(String key, String value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(
                key
                        .replace(" ", "\\ ")
                        .replace(",", "\\,")
                        .replace("=", "\\="),
                value
                        .replace("\"", "\\\"")
                        .replace("\\", "\\\\")
                        .replace("\n", "\\ ")
        );
    }

    public LineProtocolMessageBuilder appendLineProtocolField(String key, int value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolMessageBuilder appendLineProtocolField(String key, float value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolMessageBuilder appendLineProtocolField(String key, double value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolMessageBuilder appendLineProtocolField(String key, boolean value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolMessageBuilder appendLineProtocolTimestampNs(long nanos) {
        appendLineProtocolDelimiter();
        stringBuilder
                .append(nanos <= 0 ? (Instant.now().toEpochMilli() * 1_000_000) : nanos)
                .append(CHAR_UNIX_NEW_LINE);
        fieldAdded = false;
        addedLines++;
        return this;
    }

    public void calculateAndAppendFieldDelimiter() {
        if (!fieldAdded) {
            appendLineProtocolDelimiter();
        } else {
            appendLineProtocolItemDelimiter();
        }
        fieldAdded = true;
    }

    public LineProtocolMessageBuilder appendLineProtocolItemDelimiter() {
        stringBuilder.append(',');
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolRawData(String data) {
        stringBuilder.append(data);
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolDelimiter() {
        stringBuilder.append(' ');
        return this;
    }

    public LineProtocolMessageBuilder appendKeyValueDelimiter() {
        stringBuilder.append('=');
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolKeyValue(String key, String value) {
        stringBuilder
                .append(key)
                .append('=')
                .append('"')
                .append(value)
                .append('"');
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolKeyValue(String key, int value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value)
                .append('i');
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolKeyValue(String key, float value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolKeyValue(String key, double value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

    public LineProtocolMessageBuilder appendLineProtocolKeyValue(String key, boolean value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

}

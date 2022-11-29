package org.apache.jmeter.visualizers.backend.influxdb2.lineprotocol;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.enrichMsTimestamp;

public class LineProtocolBuilder {

    private static final char CHAR_UNIX_NEW_LINE = '\n';

    private final StringBuilder stringBuilder = new StringBuilder();

    private boolean fieldAdded = false;
    private int rows = 0;

    public static LineProtocolBuilder withFirstRow(
            String measurement,
            Map<String, String> tags,
            List<Map.Entry<String, Object>> fields
    ) {
        return new LineProtocolBuilder()
                .appendLineProtocolMeasurement(measurement)
                .appendTags(tags)
                .appendLineProtocolFields(fields)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
    }

    public static LineProtocolBuilder withFirstRow(
            String measurement,
            Map<String, String> tags,
            String fieldName,
            Object fieldValue
    ) {
        return new LineProtocolBuilder()
                .appendLineProtocolMeasurement(measurement)
                .appendTags(tags)
                .appendLineProtocolField(fieldName, fieldValue)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
    }

    public String build() {
        return stringBuilder.toString();
    }

    public int getRows() {
        return rows;
    }

    public LineProtocolBuilder appendRowWithTextFields(
            String measurement,
            Map<String, String> tags,
            List<Map.Entry<String, String>> fields
    ) {
        return this
                .appendLineProtocolMeasurement(measurement)
                .appendTags(tags)
                .appendLineProtocolStrFields(fields)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
    }

    public LineProtocolBuilder appendRow(
            String measurement,
            Map<String, String> tags,
            String fieldName,
            Object fieldValue
    ) {
        return this
                .appendLineProtocolMeasurement(measurement)
                .appendTags(tags)
                .appendLineProtocolField(fieldName, fieldValue)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
    }

    public LineProtocolBuilder appendRow(
            String measurement,
            Map<String, String> tags,
            List<Map.Entry<String, Object>> fields
    ) {
        return this
                .appendLineProtocolMeasurement(measurement)
                .appendTags(tags)
                .appendLineProtocolFields(fields)
                .appendLineProtocolTimestampNs(enrichMsTimestamp(Instant.now().toEpochMilli()));
    }

    public LineProtocolBuilder appendTags(Map<String, String> tags) {
        tags.forEach(this::appendLineProtocolTag);
        return this;
    }

    public LineProtocolBuilder appendLineProtocolMeasurement(String measurement) {
        return appendLineProtocolRawData(
                measurement
                        .replace(" ", "\\ ")
                        .replace(",", "\\,")
                        .replace("\n", "\\ ")
        );
    }

    public LineProtocolBuilder appendLineProtocolTag(String key, String value) {
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

    public LineProtocolBuilder appendLineProtocolStrFields(List<Map.Entry<String, String>> fields) {
        fields.forEach(
                fl -> appendLineProtocolField(fl.getKey(), fl.getValue())
        );

        return this;
    }

    public LineProtocolBuilder appendLineProtocolFields(List<Map.Entry<String, Object>> fields) {
        fields.forEach(
                fl -> appendLineProtocolField(fl.getKey(), fl.getValue())
        );

        return this;
    }

    public LineProtocolBuilder appendLineProtocolField(String key, Object value) {
        if (value instanceof Integer) {
            appendLineProtocolField(key, ((Integer) value).intValue());
        } else if (value instanceof Float) {
            appendLineProtocolField(key, ((Float) value).floatValue());
        } else if (value instanceof Double) {
            appendLineProtocolField(key, ((Double) value).doubleValue());
        } else if (value instanceof Boolean) {
            appendLineProtocolField(key, ((Boolean) value).booleanValue());
        } else {
            appendLineProtocolField(key, String.valueOf(value));
        }

        return this;
    }

    public LineProtocolBuilder appendLineProtocolField(String key, String value) {
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

    public LineProtocolBuilder appendLineProtocolField(String key, int value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolBuilder appendLineProtocolField(String key, float value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolBuilder appendLineProtocolField(String key, double value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolBuilder appendLineProtocolField(String key, boolean value) {
        calculateAndAppendFieldDelimiter();
        return appendLineProtocolKeyValue(key, value);
    }

    public LineProtocolBuilder appendLineProtocolTimestampNs(long nanos) {
        appendLineProtocolDelimiter();
        stringBuilder
                .append(nanos <= 0 ? (Instant.now().toEpochMilli() * 1_000_000) : nanos)
                .append(CHAR_UNIX_NEW_LINE);
        fieldAdded = false;
        rows++;
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

    public LineProtocolBuilder appendLineProtocolItemDelimiter() {
        stringBuilder.append(',');
        return this;
    }

    public LineProtocolBuilder appendLineProtocolRawData(String data) {
        stringBuilder.append(data);
        return this;
    }

    public LineProtocolBuilder appendLineProtocolDelimiter() {
        stringBuilder.append(' ');
        return this;
    }

    public LineProtocolBuilder appendKeyValueDelimiter() {
        stringBuilder.append('=');
        return this;
    }

    public LineProtocolBuilder appendLineProtocolKeyValue(String key, String value) {
        stringBuilder
                .append(key)
                .append('=')
                .append('"')
                .append(value)
                .append('"');
        return this;
    }

    public LineProtocolBuilder appendLineProtocolKeyValue(String key, int value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value)
                .append('i');
        return this;
    }

    public LineProtocolBuilder appendLineProtocolKeyValue(String key, float value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

    public LineProtocolBuilder appendLineProtocolKeyValue(String key, double value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

    public LineProtocolBuilder appendLineProtocolKeyValue(String key, boolean value) {
        stringBuilder
                .append(key)
                .append('=')
                .append(value);
        return this;
    }

    public LineProtocolBuilder appendLine(String text) {
        stringBuilder.append(text);
        return this;
    }

}

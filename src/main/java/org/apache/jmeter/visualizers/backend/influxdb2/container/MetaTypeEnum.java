package org.apache.jmeter.visualizers.backend.influxdb2.container;

public enum MetaTypeEnum {
    LABEL("label");
    private final String tagName;

    MetaTypeEnum(String name) {
        this.tagName = name;
    }

    public String getTagName() {
        return tagName;
    }

}

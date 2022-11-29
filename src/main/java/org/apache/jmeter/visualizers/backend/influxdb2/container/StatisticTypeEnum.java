package org.apache.jmeter.visualizers.backend.influxdb2.container;

public enum StatisticTypeEnum {

    LATENCY("latency"),
    LOAD("load"),
    ERROR("error"),
    NETWORK("network");
    private final String tagName;

    StatisticTypeEnum(String name) {
        this.tagName = name;
    }

    public String getTagName() {
        return tagName;
    }

}

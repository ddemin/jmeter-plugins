package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.commons.lang3.RandomUtils;

public class Utils {

    public static final String DELIMITER_COMPONENT_VERSION = ":";

    public static final String DELIMITER_COMPONENT_VERSION_ITEM = ",";
    public static final String TAG_LAUNCH = "launch";
    public static final String NOT_AVAILABLE = "N/A";
    public static long enrichMsTimestamp(long ms) {
        return ms * 1_000_000 + RandomUtils.nextInt(0, 999_999);
    }

}

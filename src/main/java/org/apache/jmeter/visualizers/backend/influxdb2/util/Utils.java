package org.apache.jmeter.visualizers.backend.influxdb2.util;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static final String UNDEFINED = "undefined";
    public static final String DELIMITER_KEY_VALUE = ":";
    public static final String DELIMITER_LIST_ITEM = ",";

    public static long enrichMsTimestamp(long ms) {
        return ms * 1_000_000 + RandomUtils.nextInt(0, 999_999);
    }

    @NotNull
    public static Map<String, String> parseStringToMap(String text) {
        return Arrays
                .stream(text.trim().split(DELIMITER_LIST_ITEM))
                .filter(StringUtils::isNoneEmpty)
                .map(
                        cv -> {
                            String[] arr = cv.trim().split(DELIMITER_KEY_VALUE);
                            if (arr.length > 2) {
                                LOG.error("More than one delimiter for key-value expression: " + cv);
                                return null;
                            } else if (arr.length == 2) {
                                return new AbstractMap.SimpleEntry<>(
                                        arr[0].trim(),
                                        arr[1].trim()
                                );
                            } else if (arr.length == 1) {
                                return new AbstractMap.SimpleEntry<>(
                                        arr[0].trim(),
                                        ""
                                );
                            } else {
                                return null;
                            }
                        }
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}

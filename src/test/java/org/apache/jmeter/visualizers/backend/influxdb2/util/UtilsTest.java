package org.apache.jmeter.visualizers.backend.influxdb2.util;


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class UtilsTest {

    @Test
    public void toNsPrecision() {
        long currentMs = System.currentTimeMillis();

        assert Math.floor(Utils.toNsPrecision(currentMs) / 1_000_000.0) == currentMs;

    }

    @Test
    public void parseStringToMap() {
        String mapAsString = "test1:vALUe1  , TEST2 :  value2,,test3";
        Map<String, String> expectedMap = Map.of(
                "test1", "vALUe1",
                "TEST2", "value2",
                "test3", ""
        );
        Map<String, String> actualMap = Utils.parseStringToMap(mapAsString);

        assertThat(
                actualMap,
                allOf(
                        aMapWithSize(3),
                        hasEntry("test1", "vALUe1"),
                        hasEntry("TEST2", "value2"),
                        hasEntry("test3", "")
                )
        );
    }

}
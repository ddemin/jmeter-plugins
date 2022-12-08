package org.apache.jmeter.visualizers.backend.reporter.util;


import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.UNDEFINED;
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
        String mapAsString = "test1:vALUe1  , TeST2 :  value2,,teST3";
        Map<String, Object> actualMap = Utils.toMapWithLowerCaseKey(mapAsString);

        assertThat(
                actualMap,
                allOf(
                        aMapWithSize(3),
                        hasEntry("test1", "vALUe1"),
                        hasEntry("test2", "value2"),
                        hasEntry("test3", UNDEFINED)
                )
        );
    }

}
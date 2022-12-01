package org.apache.jmeter.visualizers.backend.reporter.lineprotocol;

import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolBuilder;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;

class LineProtocolBuilderTest {

    @Test
    void firstRow() {
        String instantStr = Instant.ofEpochSecond(1).toString();

        String actualLpString = LineProtocolBuilder.withFirstRow(
                "some_measurement",
                new TreeMap<>(Map.of("tag1", "val1", "tag2", "val2")),
                List.of(
                        Map.entry("field1", 1),
                        Map.entry("field2", 2.1f),
                        Map.entry("field3", 3.2),
                        Map.entry("field4", "text4"),
                        Map.entry("field5", true),
                        Map.entry("field6", Instant.ofEpochSecond(1))
                )
        ).build();

        assertThat(
                actualLpString,
                matchesPattern(
                        "some_measurement,tag1=val1,tag2=val2 "
                                + "field1=1i,field2=2\\.1,field3=3\\.2,field4=\"text4\",field5=true,field6=\"" + instantStr
                                + "\" [0-9]{19}\\n"
                )
        );
    }

    @Test
    void nextRow() {
        String instantStr = Instant.ofEpochSecond(1).toString();

        String actualLpString = LineProtocolBuilder.withFirstRow(
                        "some_measurement",
                        new TreeMap<>(Map.of("tag1", "val1", "tag2", "val2")),
                        List.of(
                                Map.entry("field1", 1),
                                Map.entry("field2", 2.1f),
                                Map.entry("field3", 3.2),
                                Map.entry("field4", "text4"),
                                Map.entry("field5", true),
                                Map.entry("field6", Instant.ofEpochSecond(1))
                        )
                )
                .appendRow(
                        "some_measurement",
                        new TreeMap<>(Map.of("tag1", "val1", "tag2", "val2")),
                        List.of(
                                Map.entry("field1", 1),
                                Map.entry("field2", 2.1f),
                                Map.entry("field3", 3.2),
                                Map.entry("field4", "text4"),
                                Map.entry("field5", true),
                                Map.entry("field6", Instant.ofEpochSecond(1))
                        )
                )
                .build();

        assertThat(
                actualLpString,
                matchesPattern(
                        "some_measurement,tag1=val1,tag2=val2 "
                                + "field1=1i,field2=2\\.1,field3=3\\.2,field4=\"text4\",field5=true,field6=\"" + instantStr
                                + "\" [0-9]{19}\\n"
                                + "some_measurement,tag1=val1,tag2=val2 "
                                + "field1=1i,field2=2\\.1,field3=3\\.2,field4=\"text4\",field5=true,field6=\"" + instantStr
                                + "\" [0-9]{19}\\n"
                )
        );
    }

    @Test
    void measurementEscapingPositive() {
        String actualLpString = new LineProtocolBuilder().appendLineProtocolMeasurement(
                "some_measurement, with_comma\nand_new_line"
        ).build();

        assertThat(
                actualLpString,
                is("some_measurement\\,\\ with_comma\\ and_new_line")
        );
    }

    @Test
    void tagEscapingPositive() {
        String actualLpString = new LineProtocolBuilder().appendLineProtocolTag(
                "tag key,with_comma=123",
                "tag value,with_comma=456\ntest"
        ).build();

        assertThat(
                actualLpString,
                is(",tag\\ key\\,with_comma\\=123=tag\\ value\\,with_comma\\=456\\ test")
        );
    }

    @Test
    void fieldEscapingPositive() {
        String actualLpString = new LineProtocolBuilder().appendLineProtocolField(
                "field key,with_comma=123",
                "field value,\\with_\"comma\"=456\ntest"
        ).build();

        assertThat(
                actualLpString,
                is(" field\\ key\\,with_comma\\=123=\"field value,\\\\with_\\\\\"comma\\\\\"=456\\ test\"")
        );
    }

}
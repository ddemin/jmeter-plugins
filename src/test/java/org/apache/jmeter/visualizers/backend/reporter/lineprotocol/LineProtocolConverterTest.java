package org.apache.jmeter.visualizers.backend.reporter.lineprotocol;

import org.apache.jmeter.visualizers.backend.reporter.container.MetaTypeEnum;
import org.apache.jmeter.visualizers.backend.reporter.container.StatisticCounter;
import org.apache.jmeter.visualizers.backend.reporter.container.StatisticTypeEnum;
import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolBuilder.CHAR_UNIX_NEW_LINE;
import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.toNsPrecision;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

class LineProtocolConverterTest {

    LineProtocolConverter converter;
    Map<String, Object> additionalVars;
    Map<String, String> additionalLabels;

    @BeforeEach
    void setup() {
        additionalVars = new TreeMap<>(Map.of("var1", "", "var2", 123));
        additionalVars.put("var3", null);

        additionalLabels = new TreeMap<>(Map.of("label11", "", "label22", "value 22"));
        converter = new LineProtocolConverter(
                "1-1-1-1",
                "2-2-2-2",
                "host1",
                "env1",
                "profile1",
                "some environment and execution details",
                "test name",
                additionalLabels,
                60,
                30
        );
    }

    @Test
    void lpStringTestStarted() {
        String[] strArray = converter.createBuilderForTestMetadata(
                        true, additionalVars, toNsPrecision(System.currentTimeMillis())
                )
                .build()
                .split(String.valueOf(CHAR_UNIX_NEW_LINE));

        assertThat(strArray.length, is(3));

        assertThat(
                strArray[0],
                startsWith("execution,environment=env1,hostname=host1,is_it_start=true,test=1-1-1-1 uuid=\"2-2-2-2\" ")
        );
        assertThat(
                strArray[1],
                startsWith("label,test=1-1-1-1"
                        + " details=\"some environment and execution details\",name=\"test name\",profile=\"profile1\","
                        + "label11=\"undefined\",label22=\"value 22\" ")
        );
        assertThat(
                strArray[2],
                startsWith("variable,test=1-1-1-1"
                        + " warmup_sec=60i,period_sec=30i,var1=\"undefined\",var2=123i,var3=\"undefined\" ")
        );
    }

    @Test
    void lpStringForTestFinished() {
        String[] strArray = converter.createBuilderForTestMetadata(
                        false, additionalVars, toNsPrecision(System.currentTimeMillis())
                )
                .build()
                .split(String.valueOf(CHAR_UNIX_NEW_LINE));

        assertThat(strArray.length, is(1));

        assertThat(
                strArray[0],
                startsWith("execution,environment=env1,hostname=host1,is_it_start=false,test=1-1-1-1 uuid=\"2-2-2-2\" ")
        );
    }

    @Test
    void lpStringForVersions() {
        String versionsRaw = " service 1 : version 1.2.3,service 2 a b c : version 5";
        String[] strArray = converter.createBuilderForVersions(versionsRaw, toNsPrecision(System.currentTimeMillis()))
                .build()
                .split(String.valueOf(CHAR_UNIX_NEW_LINE));

        assertThat(strArray.length, is(1));

        assertThat(
                strArray[0],
                startsWith("version,test=1-1-1-1 service\\ 1=\"version 1.2.3\",service\\ 2\\ a\\ b\\ c=\"version 5\" ")
        );
    }

    @Test
    void lpStringForOperationsMetadata() {
        Map<String, Map<MetaTypeEnum, List<Map.Entry<String, String>>>> metaMap = new TreeMap<>();
        metaMap.put(
                " Service1 : GET/api/v1/test ",
                new TreeMap<>(
                        Map.of(
                                MetaTypeEnum.LABEL,
                                Arrays.asList(new AbstractMap.SimpleEntry<>("label1", "value 1 2 3"), new AbstractMap.SimpleEntry<>("label2", "5"))
                        )
                )
        );
        metaMap.put(
                " Service2 : GET/api/v1/test ",
                new TreeMap<>(
                        Map.of(
                                MetaTypeEnum.LABEL,
                                Arrays.asList(new AbstractMap.SimpleEntry<>("label11", "value 1 2 3"), new AbstractMap.SimpleEntry<>("label22", "6"))
                        )
                )
        );
        long timestampNs = toNsPrecision(System.currentTimeMillis());
        String[] strArray = converter.createBuilderForOperationsMetadata(metaMap, timestampNs)
                .build()
                .split(String.valueOf(CHAR_UNIX_NEW_LINE));
        assertThat(strArray.length, is(4));

        assertThat(
                strArray[0],
                startsWith("label,execution=2-2-2-2,operation=get/api/v1/test,target=service1"
                        + " label1=\"value 1 2 3\" " + timestampNs)
        );
        assertThat(
                strArray[1],
                startsWith("label,execution=2-2-2-2,operation=get/api/v1/test,target=service1"
                        + " label2=\"5\" " + timestampNs)
        );
        assertThat(
                strArray[2],
                startsWith("label,execution=2-2-2-2,operation=get/api/v1/test,target=service2"
                        + " label11=\"value 1 2 3\" " + timestampNs)
        );
        assertThat(
                strArray[3],
                startsWith("label,execution=2-2-2-2,operation=get/api/v1/test,target=service2"
                        + " label22=\"6\" " + timestampNs)
        );
    }

    @Test
    void lpStringForOperationsStatistic() {
        StatisticCounter latencyCounter = new StatisticCounter();
        StatisticCounter errorCounter = new StatisticCounter();
        StatisticCounter loadCounter = new StatisticCounter();
        StatisticCounter networkCounter = new StatisticCounter();
        for (int i = 1; i <= 100; i++) {
            // from 10ms to 1000ms
            latencyCounter.add(i * 10);
            // from 100 bytes per operation to 10000 bytes per operation
            networkCounter.add(i * 100);

            // 2 reported samples per operation
            loadCounter.add(2);

            // 50% error rate
            if (i % 2 == 0) {
                errorCounter.add(1);
            } else {
                errorCounter.add(0);
            }
        }

        Map<String, Map<StatisticTypeEnum, StatisticCounter>> metaMap = new TreeMap<>();
        metaMap.put(
                " Service1 : GET/api/v1/test ",
                new TreeMap<>(
                        Map.of(
                                StatisticTypeEnum.LATENCY, latencyCounter,
                                StatisticTypeEnum.ERROR, errorCounter,
                                StatisticTypeEnum.LOAD, loadCounter,
                                StatisticTypeEnum.NETWORK, networkCounter
                        )
                )
        );
        metaMap.put(
                " Service2 : GET/api/v1/test ",
                new TreeMap<>(
                        Map.of(
                                StatisticTypeEnum.LATENCY, latencyCounter,
                                StatisticTypeEnum.ERROR, errorCounter,
                                StatisticTypeEnum.LOAD, loadCounter,
                                StatisticTypeEnum.NETWORK, networkCounter
                        )
                )
        );
        long timestampNs = toNsPrecision(System.currentTimeMillis());
        String[] strArray = converter.createBuilderForOperationsStatistic(metaMap, timestampNs)
                .build()
                .split(String.valueOf(CHAR_UNIX_NEW_LINE));

        assertThat(strArray.length, is(8));

        assertThat(
                strArray[0],
                startsWith("latency,execution=2-2-2-2,operation=get/api/v1/test,target=service1 "
                        + "avg=505.0,max=1000.0,min=10.0,p50=510.0,p95=960.0,p99=1000.0 "
                        + timestampNs)
        );
        assertThat(
                strArray[1],
                startsWith("load,execution=2-2-2-2,operation=get/api/v1/test,target=service1 "
                        + "count=200.0,rate=6.6666665 "
                        + timestampNs)
        );
        assertThat(
                strArray[2],
                startsWith("error,execution=2-2-2-2,operation=get/api/v1/test,target=service1 "
                        + "count=50.0,rate=1.6666666,share=0.5 "
                        + timestampNs)
        );
        assertThat(
                strArray[3],
                startsWith("network,execution=2-2-2-2,operation=get/api/v1/test,target=service1 "
                        + "avg=5050.0,max=10000.0,min=100.0,p50=5100.0,p95=9600.0,p99=10000.0,rate=16833.334 "
                        + timestampNs)
        );

        assertThat(
                strArray[4],
                startsWith("latency,execution=2-2-2-2,operation=get/api/v1/test,target=service2 "
                        + "avg=505.0,max=1000.0,min=10.0,p50=510.0,p95=960.0,p99=1000.0 "
                        + timestampNs)
        );
        assertThat(
                strArray[5],
                startsWith("load,execution=2-2-2-2,operation=get/api/v1/test,target=service2 "
                        + "count=200.0,rate=6.6666665 "
                        + timestampNs)
        );
        assertThat(
                strArray[6],
                startsWith("error,execution=2-2-2-2,operation=get/api/v1/test,target=service2 "
                        + "count=50.0,rate=1.6666666,share=0.5 "
                        + timestampNs)
        );
        assertThat(
                strArray[7],
                startsWith("network,execution=2-2-2-2,operation=get/api/v1/test,target=service2 "
                        + "avg=5050.0,max=10000.0,min=100.0,p50=5100.0,p95=9600.0,p99=10000.0,rate=16833.334 "
                        + timestampNs)
        );
    }

}
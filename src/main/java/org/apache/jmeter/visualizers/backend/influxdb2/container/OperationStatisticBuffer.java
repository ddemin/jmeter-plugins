package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.influxdb2.container.StatisticTypeEnum.*;
import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.UNDEFINED;

public class OperationStatisticBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationStatisticBuffer.class);
    private static final String ERROR_OBFUSCATION_REGEXP
            = "("
            + "[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+"
            + "|"
            + "[0-9]+"
            + ")";
    private static final String ERROR_OBFUSCATION_PLACEHOLDER = "x";
    private static final int ERROR_MAX_CHARS = 128;

    private final Map<String, Map<StatisticTypeEnum, StatisticCounter>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    private final Map<String, Map<String, Integer>> errorMessagesBuffer
            = Collections.synchronizedMap(new HashMap<>());

    public void putMetric(SampleResult sampleResult) {
        putLatencyMetrics(sampleResult);
        putLoadMetrics(sampleResult);
        putNetworkMetrics(sampleResult);

        putErrorMetrics(sampleResult);
        if (!sampleResult.isSuccessful()) {
            putErrorMessages(sampleResult);
        }
    }

    public Map<String, Map<StatisticTypeEnum, StatisticCounter>> getStatisticBuffer() {
        return buffer;
    }

    public Map<String, Map<String, Integer>> getErrorMessagesBuffer() {
        return errorMessagesBuffer;
    }


    void putLatencyMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .computeIfAbsent(LATENCY, k -> new StatisticCounter())
                .add(sampleResult.getTime());
    }

    void putLoadMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .computeIfAbsent(LOAD, k -> new StatisticCounter())
                .add(sampleResult.getSampleCount());

    }

    void putErrorMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .computeIfAbsent(ERROR, k -> new StatisticCounter())
                .add(sampleResult.getErrorCount(), sampleResult.getSampleCount());
    }

    void putErrorMessages(SampleResult sampleResult) {
        errorMessagesBuffer
                .computeIfAbsent(
                        sampleResult.getSampleLabel(),
                        k -> new ConcurrentHashMap<>()
                )
                // TODO Think about
                .compute(getObfuscatedErrorMessage(sampleResult), (k, v) -> v == null ? 1 : v + 1);
    }

    void putNetworkMetrics(SampleResult sampleResult) {
        getResultBucket(sampleResult)
                .computeIfAbsent(NETWORK, k -> new StatisticCounter())
                .add(sampleResult.getBytesAsLong() + sampleResult.getSentBytes());
    }

    Map<StatisticTypeEnum, StatisticCounter> getResultBucket(SampleResult sampleResult) {
        return buffer
                .computeIfAbsent(
                        sampleResult.getSampleLabel(),
                        k -> new ConcurrentHashMap<>()
                );
    }

    String getObfuscatedErrorMessage(SampleResult sampleResult) {
        String errCode = StringUtils.firstNonBlank(sampleResult.getResponseCode(), UNDEFINED);
        boolean hasDigitCode = NumberUtils.isDigits(errCode);

        return (hasDigitCode ? "Error code " + errCode + ": " : "")
                + StringUtils.substring(
                        StringUtils.firstNonEmpty(
                                sampleResult.getFirstAssertionFailureMessage(),
                                sampleResult.getResponseMessage(),
                                hasDigitCode ? UNDEFINED : errCode
                        ),
                        0,
                        ERROR_MAX_CHARS
                )
                .replaceAll(ERROR_OBFUSCATION_REGEXP, ERROR_OBFUSCATION_PLACEHOLDER);
    }
}

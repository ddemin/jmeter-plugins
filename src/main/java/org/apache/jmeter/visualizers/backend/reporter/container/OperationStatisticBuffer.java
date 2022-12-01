package org.apache.jmeter.visualizers.backend.reporter.container;

import org.apache.jmeter.samplers.SampleResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.jmeter.visualizers.backend.reporter.container.StatisticTypeEnum.*;

public class OperationStatisticBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationStatisticBuffer.class);
    private final Map<String, Map<StatisticTypeEnum, StatisticCounter>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public void putMetric(SampleResult sampleResult) {
        putLatencyMetrics(sampleResult);
        putLoadMetrics(sampleResult);
        putNetworkMetrics(sampleResult);
        putErrorMetrics(sampleResult);
    }

    public Map<String, Map<StatisticTypeEnum, StatisticCounter>> getBuffer() {
        return buffer;
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

    public void clear() {
        buffer.forEach(
                (samplerName, statsMap) ->
                        statsMap.forEach(
                                (statType, counter) -> counter.clear()
                        )
        );
    }

}

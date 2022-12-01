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

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.UNDEFINED;

// TODO unit test
public class OperationErrorsBuffer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationErrorsBuffer.class);
    private static final String ERROR_OBFUSCATION_REGEXP
            = "("
            + "[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+-[0-9a-zA-Z-]+"
            + "|"
            + "[0-9]+"
            + ")";
    private static final String ERROR_OBFUSCATION_PLACEHOLDER = "x";
    private static final int ERROR_MAX_CHARS = 128;

    private final Map<String, Map<String, Integer>> buffer
            = Collections.synchronizedMap(new HashMap<>());

    public Map<String, Map<String, Integer>> getBuffer() {
        return buffer;
    }

    public void putMetric(SampleResult sampleResult) {
        if (!sampleResult.isSuccessful()) {
            putErrorMessages(sampleResult);
        }
    }

    void putErrorMessages(SampleResult sampleResult) {
        buffer
                .computeIfAbsent(
                        sampleResult.getSampleLabel(),
                        k -> new ConcurrentHashMap<>()
                )
                // TODO Think about
                .compute(getObfuscatedErrorMessage(sampleResult), (k, v) -> v == null ? 1 : v + 1);
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

    public void clear() {
        buffer.forEach(
                (samplerName, statsMap) -> statsMap.clear()
        );
    }
}

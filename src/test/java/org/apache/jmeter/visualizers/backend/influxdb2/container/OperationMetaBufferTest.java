package org.apache.jmeter.visualizers.backend.influxdb2.container;

import org.apache.jmeter.samplers.SampleResult;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jmeter.visualizers.backend.influxdb2.util.Utils.UNDEFINED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class OperationMetaBufferTest {

    private static final Logger LOG = LoggerFactory.getLogger(OperationMetaBufferTest.class);

    @Test
    void clear() {
        OperationMetaBuffer buffer = new OperationMetaBuffer();

        SampleResult someSample = new SampleResult(System.currentTimeMillis(), 1234L);
        someSample.setSampleLabel("service1: operation 1");
        someSample.setSuccessful(false);
        someSample.setResponseCode("504");
        buffer.putErrorMeta(someSample);

        assertThat(
                buffer.getBuffer(),
                aMapWithSize(1)
        );

        buffer.clear();
        assertThat(
                buffer.getBuffer(),
                aMapWithSize(0)
        );
    }

    @Test
    void putErrorMeta() {
        OperationMetaBuffer buffer = new OperationMetaBuffer();

        SampleResult withDigitCode = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithDigitCode = "service1: operation 1";
        withDigitCode.setSampleLabel(labelWithDigitCode);
        withDigitCode.setSuccessful(false);
        withDigitCode.setResponseCode("504");
        buffer.putErrorMeta(withDigitCode);

        SampleResult withDigitCodeAndMessage = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithDigitCodeAndMessage = "service11: operation 11";
        withDigitCodeAndMessage.setSampleLabel(labelWithDigitCodeAndMessage);
        withDigitCodeAndMessage.setSuccessful(false);
        withDigitCodeAndMessage.setResponseCode("504");
        withDigitCodeAndMessage.setResponseMessage("some details");
        buffer.putErrorMeta(withDigitCodeAndMessage);

        SampleResult withTextCode = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithTextCode = "service2: operation 2";
        withTextCode.setSampleLabel(labelWithTextCode);
        withTextCode.setSuccessful(false);
        withTextCode.setResponseCode("text code");
        buffer.putErrorMeta(withTextCode);

        SampleResult withTextCodeAndMessage = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithTextCodeAndMessage = "service22: operation 22";
        withTextCodeAndMessage.setSampleLabel(labelWithTextCodeAndMessage);
        withTextCodeAndMessage.setSuccessful(false);
        withTextCodeAndMessage.setResponseCode("text code");
        withTextCodeAndMessage.setResponseMessage("some details 123 45-2-2-2-12");
        buffer.putErrorMeta(withTextCodeAndMessage);

        SampleResult successful = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelSuccessful = "service3: operation 3";
        successful.setSampleLabel(labelSuccessful);
        successful.setSuccessful(true);
        successful.setResponseMessage("fail");
        successful.setResponseCode("text code");
        buffer.putErrorMeta(successful);

        assertThat(
                buffer.getBuffer(),
                aMapWithSize(4)
        );
        assertThat(
                buffer.getBuffer().get(labelWithDigitCode),
                aMapWithSize(1)
        );
        assertThat(
                buffer.getBuffer().get(labelWithDigitCode).get(MetaTypeEnum.ERROR),
                aMapWithSize(1)
        );

        assertThat(
                buffer.getBuffer().get(labelWithDigitCode).get(MetaTypeEnum.ERROR).get("description"),
                is("Error code 504: " + UNDEFINED)
        );

        assertThat(
                buffer.getBuffer().get(labelWithDigitCodeAndMessage).get(MetaTypeEnum.ERROR).get("description"),
                is("Error code 504: some details")
        );

        assertThat(
                buffer.getBuffer().get(labelWithTextCode).get(MetaTypeEnum.ERROR).get("description"),
                is("text code")
        );

        assertThat(
                buffer.getBuffer().get(labelWithTextCodeAndMessage).get(MetaTypeEnum.ERROR).get("description"),
                is("some details x x")
        );

        assertThat(
                buffer.getBuffer().get(labelSuccessful),
                nullValue()
        );
    }

    @Test
    void putLabelsMeta() {
        OperationMetaBuffer buffer = new OperationMetaBuffer();

        SampleResult sample = new SampleResult(System.currentTimeMillis(), 1234L);
        String label = "service1: operation 1";
        sample.setSampleLabel(label);
        buffer.putLabelsMeta(sample, " some label : some value , just label ,,");


        assertThat(
                buffer.getBuffer(),
                aMapWithSize(1)
        );
        assertThat(
                buffer.getBuffer().get(label),
                aMapWithSize(1)
        );
        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL),
                aMapWithSize(2)
        );

        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL).get("some label"),
                is("some value")
        );
        assertThat(
                buffer.getBuffer().get(label).get(MetaTypeEnum.LABEL).get("just label"),
                is("")
        );
    }

}
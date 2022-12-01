package org.apache.jmeter.visualizers.backend.reporter.influxdb2;

import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.samplers.StatisticalSampleResult;
import org.apache.jmeter.visualizers.backend.reporter.MetricsReportServiceScheduledTrigger;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationErrorsBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationMetaBuffer;
import org.apache.jmeter.visualizers.backend.reporter.container.OperationStatisticBuffer;
import org.apache.jmeter.visualizers.backend.reporter.influxdb2.lineprotocol.LineProtocolConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;

@WireMockTest(httpPort = 8078)
class InfluxDbServiceTest {

    MetricsReportServiceScheduledTrigger trigger;
    InfluxDbService influxDbService;
    OperationStatisticBuffer statisticBuffer;
    OperationMetaBuffer metaBuffer;
    private OperationErrorsBuffer errorsBuffer;

    @BeforeEach
    void setup() throws MalformedURLException, URISyntaxException {
        InfluxDbHttpClient influxDbHttpClient = new InfluxDbHttpClient(
                new URL("http://localhost:8078"),
                "testOrg",
                "testToken"
        );

        statisticBuffer = new OperationStatisticBuffer();
        errorsBuffer = new OperationErrorsBuffer();
        metaBuffer = new OperationMetaBuffer();

        influxDbService = new InfluxDbService(
                influxDbHttpClient,
                new LineProtocolConverter(
                        "1-1-1-1",
                        "2-2-2-2",
                        "host1",
                        "env1",
                        "profile1",
                        "some details",
                        "some name",
                        new TreeMap<>(Map.of("user-label1", "value1", "user-label2", "value2")),
                        60,
                        5
                ),
                statisticBuffer,
                errorsBuffer,
                metaBuffer,
                "testMeta",
                "operationStats",
                "operationMeta",
                new TreeMap<>(Map.of("VARIABLE_1", "value 1", "VARIABLE_2", "value 2"))
        );

        trigger = new MetricsReportServiceScheduledTrigger(
                influxDbService,
                5
        );

        stubFor(
                get(urlPathMatching("/ready"))
                        .willReturn(ok())
        );
        stubFor(
                post(urlPathMatching("/api/v2/write.*"))
                        .willReturn(ok())
        );
    }

    @Test
    void start() {
        this.influxDbService.init();

        verify(
                getRequestedFor(urlPathMatching("/ready"))
                        .withHeader("Content-Type", equalTo("plain/text"))
                        .withHeader("Authorization", equalTo("Token testToken"))
        );

        verify(
                getPatternForWriteRequestToBucket("testMeta")
                        .withRequestBody(
                                matching(
                                        "execution,environment=env1,hostname=host1,is_it_start=true,test=1-1-1-1 uuid=\"2-2-2-2\" [0-9]+\\n"
                                                + "label,test=1-1-1-1 details=\"some details\",name=\"some name\",profile=\"profile1\",user-label1=\"value1\",user-label2=\"value2\" [0-9]+\\n"
                                                + "variable,test=1-1-1-1 warmup_sec=60i,period_sec=5i,variable_1=\"value 1\",variable_2=\"value 2\" [0-9]+\\n"
                                )
                        )
        );
    }

    @Test
    void sendVersions() {
        influxDbService.sendVersions("  service1 : ver123 , service2: 234  ");

        verify(
                getPatternForWriteRequestToBucket("testMeta")
                        .withRequestBody(
                                matching("version,test=1-1-1-1 service1=\"ver123\",service2=\"234\" [0-9]+\\n")
                        )
        );
    }

    @Test
    void sendMetrics() throws InterruptedException {
        this.trigger.init();

        Thread.sleep(1000);
        resetAllRequests();

        fillBuffersWithTestData();
        verifyOperationWriteRequests(7);
    }

    @Test
    void destroy() {
        resetAllRequests();
        fillBuffersWithTestData();
        this.trigger.destroy();

        verifyOperationWriteRequests(1);

        verifyCountOfWriteRequests(1, "testMeta");
        verify(
                1,
                getPatternForWriteRequestToBucket("testMeta")
                        .withRequestBody(
                                matching(
                                        "execution,environment=env1,hostname=host1,is_it_start=false,test=1-1-1-1 uuid=\"2-2-2-2\" [0-9]+\\n"
                                )
                        )
        );
    }

    private void verifyOperationWriteRequests(int timeoutSec) {
        await()
                .pollInSameThread()
                .pollInterval(100, TimeUnit.MILLISECONDS)
                .timeout(Duration.ofSeconds(timeoutSec))
                .untilAsserted(
                        () -> {
                            verifyCountOfWriteRequests(1, "operationStats");
                            verify(
                                    1,
                                    getPatternForWriteRequestToBucket("operationStats")
                                            .withRequestBody(matching(".+\\n.+\\n.+\\n.+\\n.+\\n.+\\n.+\\n.+\\n"))
                                            .withRequestBody(matching(".*(?:|\n)error,execution=2-2-2-2,operation=operation2,target=service2 count=5.0,rate=1.0,share=0.25 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)latency,execution=2-2-2-2,operation=operation2,target=service2 avg=2345.0,max=2345.0,min=2345.0,p50=2345.0,p95=2345.0,p99=2345.0 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)load,execution=2-2-2-2,operation=operation2,target=service2 count=20.0,rate=4.0 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)network,execution=2-2-2-2,operation=operation2,target=service2 avg=4000.0,max=4000.0,min=4000.0,p50=4000.0,p95=4000.0,p99=4000.0,rate=800.0 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)error,execution=2-2-2-2,operation=operation1,target=service1 count=12.0,rate=2.4,share=0.54545456 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)latency,execution=2-2-2-2,operation=operation1,target=service1 avg=1234.0,max=1234.0,min=1234.0,p50=1234.0,p95=1234.0,p99=1234.0 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)load,execution=2-2-2-2,operation=operation1,target=service1 count=22.0,rate=4.4 [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)network,execution=2-2-2-2,operation=operation1,target=service1 avg=1000.0,max=2000.0,min=0.0,p50=2000.0,p95=2000.0,p99=2000.0,rate=800.0 [0-9]+\\n.*"))
                            );

                            verifyCountOfWriteRequests(1, "operationMeta");
                            verify(
                                    1,
                                    getPatternForWriteRequestToBucket("operationMeta")
                                            .withRequestBody(matching(".+\\n.+\\n.+\\n.+\\n.+\\n"))
                                            .withRequestBody(matching(".*(?:|\n)label,execution=2-2-2-2,operation=operation1,target=service1 somelabel=\"somevalue\" [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)label,execution=2-2-2-2,operation=operation1,target=service1 justlabel=\"undefined\" [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)error,execution=2-2-2-2,operation=operation1,reason=Error.+code.+504:.+undefined,target=service1 count=2i [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)error,execution=2-2-2-2,operation=operation1,reason=undefined,target=service1 count=2i [0-9]+\\n.*"))
                                            .withRequestBody(matching(".*(?:|\n)error,execution=2-2-2-2,operation=operation2,reason=undefined,target=service2 count=1i [0-9]+\\n.*"))
                            );
                        }
                );
    }

    private void fillBuffersWithTestData() {
        SampleResult withDigitCode = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithDigitCode = "service1: operation1";
        withDigitCode.setSampleLabel(labelWithDigitCode);
        withDigitCode.setSuccessful(false);
        withDigitCode.setResponseCode("504");
        statisticBuffer.putMetric(withDigitCode);
        errorsBuffer.putMetric(withDigitCode);

        SampleResult withDigitCode2 = new SampleResult(System.currentTimeMillis(), 1234L);
        String labelWithDigitCode2 = "service1: operation1";
        withDigitCode2.setSampleLabel(labelWithDigitCode2);
        withDigitCode2.setSuccessful(false);
        withDigitCode2.setResponseCode("504");
        statisticBuffer.putMetric(withDigitCode2);
        errorsBuffer.putMetric(withDigitCode);

        SampleResult sample = new SampleResult(System.currentTimeMillis(), 1234L);
        String label = "service1: operation1";
        sample.setSampleLabel(label);
        metaBuffer.putLabelsMeta(sample, " somelabel : somevalue , justlabel ,,");

        StatisticalSampleResult someSample = new StatisticalSampleResult(System.currentTimeMillis(), 1234L);
        String label1 = "service1: operation1";
        someSample.setSampleLabel(label1);
        someSample.setSampleCount(10);
        someSample.setSuccessful(false);
        someSample.setErrorCount(5);
        someSample.setBytes(1000);
        someSample.setSentBytes(1000);

        statisticBuffer.putMetric(someSample);
        statisticBuffer.putMetric(someSample);
        errorsBuffer.putMetric(someSample);
        errorsBuffer.putMetric(someSample);

        StatisticalSampleResult someSample2 = new StatisticalSampleResult(System.currentTimeMillis(), 2345L);
        String label2 = "service2: operation2";
        someSample2.setSampleLabel(label2);
        someSample2.setSampleCount(20);
        someSample2.setSuccessful(false);
        someSample2.setErrorCount(5);
        someSample2.setBytes(2000);
        someSample2.setSentBytes(2000);
        statisticBuffer.putMetric(someSample2);
        errorsBuffer.putMetric(someSample2);
    }

    private void verifyCountOfWriteRequests(int count, String bucket) {
        verify(
                count,
                getPatternForWriteRequestToBucket(bucket)
        );
    }

    private RequestPatternBuilder getPatternForWriteRequestToBucket(String bucket) {
        return postRequestedFor(urlPathMatching("/api/v2/write.*"))
                .withQueryParam("org", equalTo("testOrg"))
                .withQueryParam("bucket", equalTo(bucket))
                .withQueryParam("precision", equalTo("ns"))
                .withHeader("Accept", equalTo("application/json"))
                .withHeader("Content-Type", equalTo("text/plain; charset=UTF-8"))
                .withHeader("Authorization", equalTo("Token testToken"));
    }

}
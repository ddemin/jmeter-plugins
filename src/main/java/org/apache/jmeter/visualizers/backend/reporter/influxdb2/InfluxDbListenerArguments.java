package org.apache.jmeter.visualizers.backend.reporter.influxdb2;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.apache.jmeter.visualizers.backend.reporter.AbstractListenerArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.UNDEFINED;

public class InfluxDbListenerArguments extends AbstractListenerArguments {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbListenerArguments.class);

    private static final String ARG_INFLUXDB2_URL = "InfluxdDB2 gateway URL";
    private static final String ARG_INFLUXDB2_TOKEN = "InfluxdDB2 access token";
    private static final String ARG_INFLUXDB2_ORG = "InfluxdDB2 organization name";
    private static final String ARG_INFLUXDB2_BUCKET_TEST_META = "InfluxdDB2 bucket name for test metadata";
    private static final String ARG_INFLUXDB2_BUCKET_OPERATION_STATS
            = "InfluxdDB2 bucket name for test operations statistic";
    private static final String ARG_INFLUXDB2_BUCKET_OPERATION_META
            = "InfluxdDB2 bucket name for test operations metadata";

    static {
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_URL, "https://host:port");
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_TOKEN, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_ORG, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_TEST_META, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_OPERATION_STATS, UNDEFINED);
        ARGUMENTS_DEFAULT.addArgument(ARG_INFLUXDB2_BUCKET_OPERATION_META, UNDEFINED);
    }

    private final URL influxDbUrl;
    private final String influxDbToken;
    private final String influxDbOrg;
    private final String influxDbBucketTestMeta;
    private final String influxDbBucketOperationStats;
    private final String influxDbBucketOperationMeta;

    static Arguments getDefaultArguments() {
        return ARGUMENTS_DEFAULT;
    }

    InfluxDbListenerArguments(BackendListenerContext context) throws Exception {
        super(context);
        this.influxDbUrl = new URL(context.getParameter(ARG_INFLUXDB2_URL));
        this.influxDbToken = context.getParameter(ARG_INFLUXDB2_TOKEN);
        this.influxDbOrg = context.getParameter(ARG_INFLUXDB2_ORG);
        this.influxDbBucketTestMeta = context.getParameter(ARG_INFLUXDB2_BUCKET_TEST_META);
        this.influxDbBucketOperationStats = context.getParameter(ARG_INFLUXDB2_BUCKET_OPERATION_STATS);
        this.influxDbBucketOperationMeta = context.getParameter(ARG_INFLUXDB2_BUCKET_OPERATION_META);
    }

    public URL getInfluxDbUrl() {
        return this.influxDbUrl;
    }

    public String getInfluxDbToken() {
        return this.influxDbToken;
    }

    public String getInfluxDbOrg() {
        return this.influxDbOrg;
    }

    public String getInfluxDbBucketTestMeta() {
        return this.influxDbBucketTestMeta;
    }

    public String getInfluxDbBucketOperationStats() {
        return this.influxDbBucketOperationStats;
    }

    public String getInfluxDbBucketOperationMeta() {
        return this.influxDbBucketOperationMeta;
    }

}

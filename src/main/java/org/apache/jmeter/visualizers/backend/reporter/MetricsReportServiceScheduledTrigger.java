package org.apache.jmeter.visualizers.backend.reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.jmeter.visualizers.backend.reporter.util.Utils.toNsPrecision;

public class MetricsReportServiceScheduledTrigger implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsReportServiceScheduledTrigger.class);

    private final AbstractMetricsReportService reporterService;
    private final ScheduledExecutorService scheduler;
    private final long batchingPeriod;
    private final boolean isItPrimaryJMeter;

    private ScheduledFuture<?> timerHandle;

    public MetricsReportServiceScheduledTrigger(
            boolean isItPrimaryJMeter, AbstractMetricsReportService reporterService, int batchingPeriod
    ) {
        this.isItPrimaryJMeter = isItPrimaryJMeter;
        this.reporterService = reporterService;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.batchingPeriod = batchingPeriod;
    }

    @Override
    public void run() {
        reporterService.collectOperationsLabels();

        long timestampNs = toNsPrecision(System.currentTimeMillis());
        if (isItPrimaryJMeter) {
            reporterService.collectAndSendTags(timestampNs);
            reporterService.collectAndSendVersions(timestampNs);
        }

        reporterService.sendOperationsMetrics(timestampNs);

        reporterService.retryFailedRequests();
    }

    public void init() {
        timerHandle = scheduler.scheduleAtFixedRate(
                this,
                batchingPeriod,
                batchingPeriod,
                TimeUnit.SECONDS
        );
        reporterService.init();
    }

    public void destroy() {
        try {
            if (timerHandle != null) {
                timerHandle.cancel(false);
                if (scheduler != null) {
                    scheduler.shutdown();
                    try {
                        scheduler.awaitTermination(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        LOG.error("Error waiting for end of scheduler");
                        Thread.currentThread().interrupt();
                    }
                }
            }
        } catch (Throwable tr) {
            LOG.error("Something goes wrong during InfluxDB integration teardown: " + tr.getMessage(), tr);
        }

        reporterService.destroy();
    }

}

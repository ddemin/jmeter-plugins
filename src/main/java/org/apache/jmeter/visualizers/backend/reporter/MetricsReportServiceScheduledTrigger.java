package org.apache.jmeter.visualizers.backend.reporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class MetricsReportServiceScheduledTrigger implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsReportServiceScheduledTrigger.class);

    private final AbstractMetricsReportService reporterService;
    private final ScheduledExecutorService scheduler;
    private final long batchingPeriod;

    private ScheduledFuture<?> timerHandle;

    public MetricsReportServiceScheduledTrigger(AbstractMetricsReportService reporterService, int batchingPeriod) {
        this.reporterService = reporterService;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.batchingPeriod = batchingPeriod;
    }

    @Override
    public void run() {
        reporterService.collectAndSendVersions();
        reporterService.collectOperationsLabels();

        reporterService.sendOperationsMetrics();

        reporterService.processRetryQueue();
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

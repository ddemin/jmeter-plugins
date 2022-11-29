package org.apache.jmeter.visualizers.backend.influxdb2.influx;

import org.apache.commons.lang3.StringUtils;
import org.apache.jmeter.visualizers.backend.influxdb2.util.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;

public class InfluxDbHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbHttpClient.class);
    private static final long RETRY_QUEUE_MAX_CAPACITY = 1_000;
    private static final int RETRY_ATTEMPTS_MAX = 5;
    private static final int RETRY_PAUSE_MS = 1_000;
    private static final int CONNECT_ATTEMPTS_MAX = 3;
    private static final int CONNECT_ATTEMPTS_PAUSE_MS = 3_000;
    private static final int HTTP_REQUEST_TIMEOUT_SEC = 10;

    private final URI uri;
    private final String org;
    private final String tokenHeader;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final Map<String, URI> uriCache = new ConcurrentHashMap<>();
    private final Queue<HttpRequest> retryQueue = new ConcurrentLinkedQueue<>();
    private final HttpRequest.Builder writeRequestTemplate;
    private final HttpRequest.Builder readyRequestTemplate;

    public InfluxDbHttpClient(URL url, String org, String token) throws URISyntaxException {
        this.uri = url.toURI();
        this.org = org;
        this.tokenHeader = "Token " + token;

        this.readyRequestTemplate = HttpRequest.newBuilder()
                .GET()
                .uri(uri.resolve("/ready"))
                .header("Content-Type", "plain/text")
                .header("Authorization", tokenHeader)
                .timeout(Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SEC));

        this.writeRequestTemplate = HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(HTTP_REQUEST_TIMEOUT_SEC))
                .version(HttpClient.Version.HTTP_1_1)
                .header("Accept", "application/json")
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("Authorization", tokenHeader);
    }

    public boolean isConnected() {
        for(int attempts = 0; attempts < CONNECT_ATTEMPTS_MAX; attempts++) {
            if (tryToConnect()) {
                return true;
            }
            try {
                Thread.sleep(CONNECT_ATTEMPTS_PAUSE_MS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return false;
    }

    public void processRetryQueue() {
        int attemptNo = 0;
        while (retryQueue.peek() != null && attemptNo < RETRY_ATTEMPTS_MAX) {
            HttpRequest requestForRetry = retryQueue.peek();
            try {
                Thread.sleep(RETRY_PAUSE_MS * attemptNo);
                send(requestForRetry);
                retryQueue.poll();
            } catch (InterruptedException | RetriableException e) {
                attemptNo++;
            }
        }

        if (attemptNo >= RETRY_ATTEMPTS_MAX) {
            LOG.error("Couldn't send data to InfluxDB with max retry attempts: " + RETRY_ATTEMPTS_MAX);
        }
    }

    public void send(String bucket, String lineProtocolContent) throws RetriableException {
        var request = buildWriteRequest(bucket, lineProtocolContent);
        send(request);
    }

    void send(HttpRequest request) throws RetriableException {
        try {
            tryToSend(request);
        } catch (RetriableException ex) {
            LOG.error(ex.getMessage());
            tryToRememberForRetry(request);
            throw ex;
        }
    }

    void tryToSend(HttpRequest httpRequest) throws RetriableException {
        HttpResponse<String> response;
        try {
            LOG.debug("Try to send HTTP request: " + httpRequest.toString());
            response = httpClient.send(
                    httpRequest,
                    HttpResponse.BodyHandlers.ofString()
            );
            LOG.debug("HTTP request was send");
        } catch (SocketTimeoutException | HttpTimeoutException | InterruptedException ex) {
            throw new RetriableException("Can't send metric to InfluxDB", ex);
        } catch (IOException ioex) {
            if (StringUtils.isNotEmpty(ioex.getMessage()) && ioex.getMessage().contains("too many")) {
                LOG.error("Can't send metric to InfluxDB", ioex);
                throw new RetriableException("Can't send metric to InfluxDB", ioex);
            }
            throw new RuntimeException(ioex);
        }

        if (response != null && response.statusCode() >= 400 && response.statusCode() < 500) {
            throw new IllegalStateException(
                    "HTTP body: " + response.body() + ", HTTP code: " + response.statusCode()
            );
        } else if (response != null && response.statusCode() >= 500) {
            throw new RetriableException("Can't send metric to InfluxDB because of HTTP 5xx: " + response.body());
        }
    }

    void tryToRememberForRetry(HttpRequest httpRequest) {
        if (retryQueue.size() < RETRY_QUEUE_MAX_CAPACITY) {
            retryQueue.add(httpRequest);
        } else {
            LOG.error("Can't put more requests for retry to the queue - limit is reached: " + RETRY_QUEUE_MAX_CAPACITY);
        }
    }

    HttpRequest buildWriteRequest(String bucket, String lineProtocolContent) {
        LOG.debug("Try to build HTTP request");
        return writeRequestTemplate
                .uri(
                        uriCache.computeIfAbsent(
                                bucket,
                                k ->
                                        uri.resolve(
                                                "/api/v2/write?"
                                                        + "org=" + org
                                                        + "&bucket=" + bucket
                                                        + "&precision=ns"
                                        )
                        )
                )
                .POST(HttpRequest.BodyPublishers.ofString(lineProtocolContent, UTF_8))
                .build();
    }

    boolean tryToConnect() {
        LOG.info("Check InfluxDB connection ...");

        HttpRequest authRequest = readyRequestTemplate.build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(authRequest, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOG.error("Can't connect to InfluxDB2: " + e.getMessage(), e);
            return false;
        }

        if (response.statusCode() == HTTP_OK) {
            LOG.info("Connection is established, user is authorized");
            return true;
        } else {
            LOG.error(
                    "Can't authorize to InfluxDB2, HTTP response: "
                            + response.body()
                            + ", HTTP code: "
                            + response.statusCode()
            );
            return false;
        }
    }

}

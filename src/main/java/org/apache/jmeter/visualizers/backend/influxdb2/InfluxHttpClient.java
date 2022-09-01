package org.apache.jmeter.visualizers.backend.influxdb2;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.nio.charset.StandardCharsets.UTF_8;

public class InfluxHttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(InfluxHttpClient.class);

    private final String url;
    private final String org;
    private final String tokenHeader;
    private final HttpClient httpClient = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10))
            .build();
    private final Map<String, URI> uriCache = new ConcurrentHashMap<>();
    private boolean isConnected;

    public InfluxHttpClient(String url, String org, String token) {
        this.url = StringUtils.removeEnd(url.trim(), "/");
        this.org = org;
        this.tokenHeader = "Token " + token;
        this.isConnected = tryToConnect();
    }

    public boolean tryToSend(String bucket, String lineProtocolContent) throws InterruptedException {
        LOG.debug("Try to build HTTP request");

        HttpRequest httpRequest = buildWriteRequest(bucket, lineProtocolContent);
        return tryToSend(httpRequest);
    }

    public boolean isConnected() {
        return isConnected;
    }

    private boolean tryToConnect() {
        HttpRequest authRequest = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(url + "/ready"))
                .header("Content-Type", "plain/text")
                .header("Authorization", tokenHeader)
                .timeout(Duration.ofSeconds(10))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(authRequest, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            LOG.error("Can't connect to InfluxDB2: " + e.getMessage(), e);
            return false;
        }

        if (response.statusCode() == 200) {
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

    private boolean tryToSend(HttpRequest httpRequest) throws InterruptedException {
        HttpResponse<String> response;
        try {
            LOG.debug("Try to send HTTP request: " + httpRequest.toString());
            response = httpClient.send(
                    httpRequest,
                    HttpResponse.BodyHandlers.ofString()
            );
            LOG.debug("HTTP request was send");
        } catch (SocketTimeoutException | HttpTimeoutException ex) {
            LOG.error("Can't send metric to InfluxDB", ex);
            return false;
        } catch (IOException ioex) {
            if (StringUtils.isNotEmpty(ioex.getMessage()) && ioex.getMessage().contains("too many")) {
                LOG.error("Can't send metric to InfluxDB", ioex);
                return false;
            }
            throw new RuntimeException(ioex);
        }

        if (response != null && response.statusCode() >= 400 && response.statusCode() < 500) {
            throw new IllegalStateException(
                    "HTTP body: " + response.body() + ", HTTP code: " + response.statusCode()
            );
        } else if (response != null && response.statusCode() >= 500) {
            LOG.error("Can't send metric to InfluxDB because of HTTP 500: " + response.body());
            return false;
        }

        return true;
    }

    private HttpRequest buildWriteRequest(String bucket, String lineProtocolContent) {
        return HttpRequest.newBuilder()
                .uri(
                        uriCache.computeIfAbsent(
                                bucket,
                                k ->
                                        URI.create(
                                                url
                                                        + "/api/v2/write?"
                                                        + "org=" + org
                                                        + "&bucket=" + bucket
                                                        + "&precision=ns"
                                        )
                        )
                )
                .timeout(Duration.ofSeconds(5))
                .version(HttpClient.Version.HTTP_1_1)
                .header("Accept", "application/json")
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("Authorization", tokenHeader)
                .POST(HttpRequest.BodyPublishers.ofString(lineProtocolContent, UTF_8))
                .build();
    }

}

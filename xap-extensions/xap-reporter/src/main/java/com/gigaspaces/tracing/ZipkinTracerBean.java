package com.gigaspaces.tracing;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.gigaspaces.start.SystemInfo;
import com.gigaspaces.admin.ManagerClusterType;
import io.opentracing.util.GlobalTracer;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipkinTracerBean {
    public static final String CONSUL_KEY = "gigaspaces/tracing";
    private Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private AsyncReporter<Span> reporter;
    private Tracing tracing;
    private BraveTracer tracer;
    private Thread thread;
    private boolean useConsul;
    private boolean startActive = false;
    private String serviceName;
    private String zipkinUrl = "http://zipkin.service.consul:9411";

    public ConsulClient getClient(String agentHost) {
        try {

            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
                    builder.build());
            CloseableHttpClient customHttpClient = HttpClients.custom().setSSLSocketFactory(
                    sslsf).build();
            ConsulRawClient rawClient = new ConsulRawClient(agentHost, customHttpClient);


            return new ConsulClient(rawClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ZipkinTracerBean() {
        if (GlobalTracer.isRegistered()) throw new IllegalArgumentException("GlobalTracer already exists");
        useConsul = SystemInfo.singleton().getManagerClusterInfo().getManagerClusterType() == ManagerClusterType.ELASTIC_GRID;
    }

    public ZipkinTracerBean(String serviceName) {
        this();
        this.serviceName = serviceName;
    }

    public ZipkinTracerBean setStartActive(boolean startActive) {
        this.startActive = startActive;
        return this;
    }

    public ZipkinTracerBean setZipkinUrl(String zipkinUrl) {
        this.zipkinUrl = zipkinUrl;
        return this;
    }

    @PostConstruct
    public void start() {
        logger.info("Starting " + (startActive ? "active" : "inactive") + " with service name [" + serviceName + "]");
        logger.info("Connecting to Zipkin at " + zipkinUrl);
        if (useConsul) {
            logger.info("Using Consul for turning tracing on/off, key is: " + CONSUL_KEY);
        }


        OkHttpSender sender = OkHttpSender.create(
                zipkinUrl + "/api/v2/spans");
        reporter = AsyncReporter.builder(sender).build();
        tracing = Tracing.newBuilder()
                .localServiceName(serviceName)
                .spanReporter(reporter)
                .build();
        if (!startActive) {
            tracing.setNoop(true);
        }

        tracer = BraveTracer.create(tracing);
        GlobalTracer.registerIfAbsent(tracer);

        if (useConsul) {

            thread = new Thread(new Runnable() {
                private final ConsulClient client = getClient("https://localhost:8500");

                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Response<GetValue> kvClient = client.getKVValue(CONSUL_KEY);
                            if (kvClient.getValue() != null) {
                                boolean tracingIsOn = Boolean.parseBoolean(kvClient.getValue().getDecodedValue());
                                if (tracingIsOn && tracing.isNoop()) {
                                    logger.info("Turning tracing on");
                                    tracing.setNoop(false);
                                } else if (!tracingIsOn && !tracing.isNoop()) {
                                    logger.info("Turning tracing off");
                                    tracing.setNoop(true);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("Got exception while querying Consul", e);
                        }
                        try {
                            TimeUnit.SECONDS.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            });

            thread.start();
        }
    }


    @PreDestroy
    public void destroy() throws Exception {
        if (thread != null) {
            thread.interrupt();
        }

        if (tracer != null) {
            tracer.close();
        }

        if (reporter != null) {
            reporter.close();
        }

    }
}

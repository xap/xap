package com.gigaspaces.tracing;

import brave.Tracing;
import brave.opentracing.BraveTracer;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import io.opentracing.util.GlobalTracer;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.okhttp3.OkHttpSender;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZipkinTracerBean implements InitializingBean, DisposableBean {
    private static final String CONSUL_KEY = "gigaspaces/tracing";
    private Logger logger = Logger.getLogger(this.getClass().getName());

    private AsyncReporter<Span> reporter;
    private Tracing tracing;
    private BraveTracer tracer;
    private Thread thread;
    private boolean useConsul = true;
    private boolean startActive = true;
    private String serviceName;
    private String zipkinUrl = "http://zipkin.service.consul:9411";

    private ZipkinTracerBean() {
        if (GlobalTracer.isRegistered()) throw new IllegalArgumentException("GlobalTracer already exists");
    }

    public ZipkinTracerBean(String serviceName) {
        this();
        this.serviceName = serviceName;
    }

    public ZipkinTracerBean setUseConsul(boolean useConsul) {
        this.useConsul = useConsul;
        return this;
    }

    public ZipkinTracerBean setStartActive(boolean startActive) {
        this.startActive = startActive;
        return this;
    }

    public ZipkinTracerBean setZipkinUrl(String zipkinUrl) {
        this.zipkinUrl = zipkinUrl;
        return this;
    }

    @Override
    public void afterPropertiesSet() {
        logger.info("Starting " + (startActive ? "active" : "inactive") + " with service name [" + serviceName + "]");
        logger.info("Connecting to Zepking at " + zipkinUrl);
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
                private final ConsulClient client = new ConsulClient("localhost");

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
                            logger.log(Level.SEVERE, "Got exception while querying Consul", e);
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


    @Override
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

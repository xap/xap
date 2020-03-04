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

public class ZipkinTracerBean implements InitializingBean, DisposableBean {

    private AsyncReporter<Span> reporter;
    private Tracing tracing;
    private BraveTracer tracer;
    private Thread thread;
    private boolean consulPlugin = true;
    private boolean startActive = false;
    private String serviceName;

    public ZipkinTracerBean() {
        if (GlobalTracer.isRegistered()) throw new IllegalArgumentException("GlobalTracer already exists");
    }

    public ZipkinTracerBean(String serviceName) {
        this();
        this.serviceName = serviceName;
    }

    public ZipkinTracerBean(String serviceName, boolean startActive) {
        this(serviceName);
        this.startActive = startActive;
    }

    @Override
    public void afterPropertiesSet() {
        OkHttpSender sender = OkHttpSender.create(
                "http://zipkin.service.consul:9411/api/v2/spans");
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

        if (consulPlugin) {

            thread = new Thread(new Runnable() {
                private final ConsulClient client = new ConsulClient("localhost");

                @Override
                public void run() {
                    while (!Thread.currentThread().isInterrupted()) {
                        Response<GetValue> kvClient = client.getKVValue("gigaspaces/tracing");
                        if (kvClient.getValue() != null) {
                            System.out.println("Tracing value: " + kvClient.getValue().getDecodedValue());
                            boolean tracingIsOn = Boolean.parseBoolean(kvClient.getValue().getDecodedValue());
                            if (tracingIsOn && tracing.isNoop()) {
                                System.out.println("Tracing was set to on...");
                                tracing.setNoop(false);
                            } else if (!tracingIsOn && !tracing.isNoop()) {
                                System.out.println("Tracing was set to off...");
                                tracing.setNoop(true);
                            }
                        }
                        try {
                            TimeUnit.SECONDS.sleep(10);
                        } catch (InterruptedException e) {
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

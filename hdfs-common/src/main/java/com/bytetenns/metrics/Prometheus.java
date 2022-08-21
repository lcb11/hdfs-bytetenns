package com.bytetenns.metrics;

import com.bytetenns.utils.NetUtils;
import io.prometheus.client.*;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 监控节点选项
 *
 * @author
 */
@Slf4j
public class Prometheus {

    private static Map<String, Gauge> gaugeMap = new ConcurrentHashMap<>();
    private static Map<String, Counter> counterMap = new ConcurrentHashMap<>();
    private static Map<String, Histogram> histogramMap = new ConcurrentHashMap<>();
    private static Map<String, QpsGauge> qpsStatMap = new ConcurrentHashMap<>();

    private Prometheus() {
    }

    public static void initializeHotspot() {
        DefaultExports.initialize();
    }

    public static void gauge(String metricsName, String help, String labelNames, String label, double value) {
        try {
            synchronized (Prometheus.class) {
                Gauge gauge = gaugeMap.get(metricsName);
                if (gauge == null) {
                    gauge = Gauge.build()
                            .name(metricsName)
                            .help(help)
                            .labelNames(labelNames, "hostname")
                            .register();
                    gaugeMap.put(metricsName, gauge);
                }
                gauge.labels(label, NetUtils.getHostName()).set(value);
            }
        } catch (Exception e) {
            log.warn("Metrics error.", e);
        }
    }

    public static void incCounter(String metricsName, String help) {
        incCounter(metricsName, help, 1);
    }

    public static void hit(String metricsName, String help) {
        hit(metricsName, help, 1);
    }

    public static void hit(String metricsName, String help, int number) {
        synchronized (Prometheus.class) {
            QpsGauge qpsGauge = qpsStatMap.get(metricsName);
            if (qpsGauge == null) {
                Gauge gauge = Gauge.build()
                        .name(metricsName)
                        .help(help)
                        .labelNames("hostname")
                        .create();
                qpsGauge = new QpsGauge(gauge);
                qpsStatMap.put(metricsName, qpsGauge);
            }
            qpsGauge.hit(number);
        }
    }


    public static void incCounter(String metricsName, String help,double amt) {
        try {
            synchronized (Prometheus.class) {
                Counter counter = counterMap.get(metricsName);
                if (counter == null) {
                    counter = Counter.build()
                            .name(metricsName)
                            .help(help)
                            .labelNames("hostname")
                            .register();
                    counterMap.put(metricsName, counter);
                }
                counter.labels(NetUtils.getHostName()).inc(amt);
            }
        } catch (Exception e) {
            log.warn("Metrics error.", e);
        }
    }

    public static Histogram histogram(String name) {
        synchronized (Prometheus.class) {
            Histogram histogram = histogramMap.get(name);
            if (histogram == null) {
                histogram = Histogram.build()
                        .name(name)
                        .help(name)
                        .register();
                histogramMap.put(name, histogram);
            }
            return histogram;
        }
    }

    private static class QpsGauge extends Collector {
        private Gauge gauge;
        private RollingWindow rollingWindow;

        public QpsGauge(Gauge gauge) {
            this.gauge = gauge;
            this.rollingWindow = new RollingWindow();
            CollectorRegistry.defaultRegistry.register(this);
        }

        @Override
        public List<MetricFamilySamples> collect() {
            long currentQps = rollingWindow.getCurrentQps();
            gauge.labels(NetUtils.getHostName()).set(currentQps);
            return gauge.collect();
        }

        public void hit(int number) {
            rollingWindow.hit(number);
        }
    }
}

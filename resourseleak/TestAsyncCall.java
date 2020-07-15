package resourceleak;


import com.amazon.coral.metrics.Metrics;

import javax.measure.unit.SI;
import javax.measure.unit.Unit;
import javax.ws.rs.client.AsyncInvoker;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;


public class TestAsyncCall {

    public void testAsyncCall() {



        Metrics metrics = metricsFactory.newMetrics();

        long start = System.currentTimeMillis();
        metrics.addProperty("Program", "SLRRouterService");
        metrics.addProperty("Operation", "AsyncHealthCheck");
        if (skipHealthCheck()) {
            metrics.addLevel(serviceRouter.getService() + ".SkippedHealthCheckRate", 1, Unit.ONE);
            metrics.addDate("StartTime", start);
            metrics.addDate("EndTime", start);
            metrics.addTime("Time", 0, SI.MILLI(SI.SECOND));
            metrics.close();
            return;
        }
        metrics.addProperty("Operation", "AsyncHealthCheck");
        metrics.addLevel(serviceRouter.getService() + ".SkippedHealthCheckRate", 0, Unit.ONE);

        AsyncInvoker asyncInvoker = serverTarget.request().async();
        asyncInvoker.get(i -> {
            int i = 0;
            for (i =0; i < 10; i++) {
                System.out.println("%d", i);
            }
            metrics.close();

                         });

        asyncInvoker.get(new InvocationCallback<Response>() {
            @Override
            public void completed(Response o) {
                try {
                    if (o.getStatus() == HTTP_SUCCESS_CODE) {
                        metrics.addLevel(serviceRouter.getService() + ".SucceededHealthCheckRate", 1, Unit.ONE);
                        healthCheckSucceeded();
                    } else {
                        metrics.addLevel(serviceRouter.getService() + ".SucceededHealthCheckRate", 0, Unit.ONE);
                        healthCheckFailed();
                    }
                } finally {
                    o.close();
                    closeMetrics();
                }
            }

            @Override
            public void failed(Throwable throwable) {
                try {
                    metrics.addLevel("SucceededHealthCheckRate", 0, Unit.ONE);
                    healthCheckFailed();
                } finally {
                    closeMetrics();
                }
            }

            void closeMetrics() {
                long end = System.currentTimeMillis();
                metrics.addDate("StartTime", start);
                metrics.addDate("EndTime", end);
                metrics.addTime("Time", end - start, SI.MILLI(SI.SECOND));
                metrics.close();
            }
        });
    }

}

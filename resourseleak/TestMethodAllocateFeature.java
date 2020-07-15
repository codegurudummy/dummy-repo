package resourceleak;

import com.amazon.coral.metrics.Metrics;
import com.amazon.coral.reflect.InvocationException;
import com.amazon.coral.reflect.Invoker;
import com.amazon.coral.service.Activity;
import com.amazon.sable.constants.Headers;
import com.amazon.sable.netty.context.RequestContext;
import com.amazon.sable.netty.framing.stumpy.Message;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import javax.measure.unit.Unit;


public class TestMethodAllocateFeature extends SimpleChannelUpstreamHandler {

    @Override
    public Object testMethodAllocateFeature1(Invoker invoker, Object activity, Object... parameters) throws InvocationException {
        Metrics metrics = ((Activity)activity).getMetrics();
        Object request = parameters.length > 0 ? parameters[0] : null;
        if (request != null) {
            addProperty(metrics,"RequestAwsAccountId", getPropertyValue(request, "getAccountId"));
            addProperty(metrics,"RequestResource", getPropertyValue(request, "getResource"));
            addProperty(metrics,"RequestTrainingJobArn", getPropertyValue(request, "getTrainingJob", "getTrainingJobArn"));
            addProperty(metrics, "ResourceArn", getPropertyValue(request, "getResourceArn"));
        }

        return invoker.invoke(activity, parameters);
    }

    @Override
    public void testMethodAllocateFeature2(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        final Object obj = e.getMessage();

        if (obj instanceof RequestContext) {
            final RequestContext requestContext = (RequestContext)obj;
            final Message message = requestContext.getOriginalMessage();
            final Metrics metrics = requestContext.getMetrics();

            if (message != null) {
                metrics.addCount("XRegionCall:" + message.getVerb() + ":Count", 1, Unit.ONE);
                final String scope = message.getHeader(Headers.SCOPE);
                if (scope != null) {
                    metrics.addCount("XRegionCall:" + message.getVerb() + ":" + scope + ":Count", 1, Unit.ONE);
                }
            }
        }

        // pass upstream
        super.messageReceived(ctx, e);
    }

    public Object testMethodAllocateFeature3(final Invoker invoker, final Object target, final Object... parameters)
            throws InvocationException {
        Activity targetActivity = (Activity)target;
        Metrics metrics = targetActivity.getMetrics();
        try {
            Object result = invoker.invoke(target, parameters);
            metrics.addCount("Success", 1, Unit.ONE);
            return result;
        } catch (InvocationException invocationException) {
            metrics.addCount(String.format("Exception:%s", invocationException.getCause().getClass().getSimpleName()),
                    1, Unit.ONE);
            throw invocationException;
        }
    }


}

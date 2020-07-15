package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.InvokeRequest;

import java.io.IOException;

public class Example implements RequestHandler<ScheduledEvent, Void> {

    private AWSLambda lambdaClient;
    private MyOtherClient otherClient;

    // Should flag
    public void retry() {
        final InvokeRequest request = new InvokeRequest();
        while (true) {
            try {
                return lambdaClient.invoke(request);
            } catch (AmazonServiceException e) {
            }
        }
    }

    // Should flag
    public void retryInDoWhile() {
        final InvokeRequest request = new InvokeRequest();
        do {
            try {
                return lambdaClient.invoke(request);
            } catch (AmazonServiceException e) {
            }
        } while (true);
    }

    // Should not flag
    public void loopThenCatch() {
        final InvokeRequest request = new InvokeRequest();
        try {
            while (true) {
                return lambdaClient.invoke(request);
            }
        } catch (AmazonServiceException e) {
        }
    }

    // Should not flag
    public void noLoop() {
        final InvokeRequest request = new InvokeRequest();
        try {
            return lambdaClient.invoke(request);
        } catch (AmazonServiceException e) {
        }
    }

    // Should not flag
    public void noTry() {
        final InvokeRequest request = new InvokeRequest();
        while (true) {
            return lambdaClient.invoke(request);
        }
    }

    // Should not flag
    public void notAwsClient() {
        final InvokeRequest request = new InvokeRequest();
        while (true) {
            try {
                return otherClient.foo(request);
            } catch (AmazonServiceException e) {
            }
        }
    }

    // Should not flag
    public void otherError() {
        final InvokeRequest request = new InvokeRequest();
        while (true) {
            try {
                otherClient.foo();
                return lambdaClient.invoke(request);
            } catch (IOException e) {
            }
        }
    }

    // Should not flag
    public void differentInputs() {
        while (true) {
            final InvokeRequest request = otherClient.foo();
            try {
                return lambdaClient.invoke(request);
            } catch (AmazonServiceException e) {
            }
        }
    }

    // Should not flag
    public void differentInputsDoWhile() {
        do {
            final InvokeRequest request = otherClient.foo();
            try {
                return lambdaClient.invoke(request);
            } catch (AmazonServiceException e) {
            }
        } while (true);
    }

    // Should not flag
    public void inputChanges() {
        final InvokeRequest request = new InvokeRequest();
        while (true) {
            request.setPayload(otherClient.foo());
            try {
                return lambdaClient.invoke(request);
            } catch (AmazonServiceException e) {
            }
        }
    }
}
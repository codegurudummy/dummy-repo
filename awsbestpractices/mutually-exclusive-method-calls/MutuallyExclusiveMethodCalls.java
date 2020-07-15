package example;

import com.amazonaws.services.kms.model.GenerateDataKeyRequest;

public class Example {
    // Based on
    // https://code.amazon.com/packages/PinguServiceCrypto/blobs/9eb45868edb542dc8d042a4da18ad91f137593c2/--/src/com/amazon/kms/pingu/crypto/PinguKeystoreCrypto.java#L709-L710
    // Must flag
    private GenerateDataKeyRequest setKeySpecAndNumberOfBytes(String keySpec, Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest = new GenerateDataKeyRequest();
        generateRequest.setKeySpec(keySpec);
        generateRequest.setNumberOfBytes(numberOfBytes);
        return generateRequest;
    }

    // Must flag
    private GenerateDataKeyRequest withKeySpecAndNumberOfBytes(String keySpec, Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest = new GenerateDataKeyRequest()
                .withNumberOfBytes(numberOfBytes)
                .withFoo("bar")
                .withKeySpec(keySpec);
        return generateRequest;
    }

    // Must flag
    private void multipleDetections(String keySpec, Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest1 = new GenerateDataKeyRequest();
        generateRequest1.setKeySpec(keySpec);
        generateRequest1.setNumberOfBytes(numberOfBytes);

        GenerateDataKeyRequest generateRequest2 = new GenerateDataKeyRequest();
        generateRequest2.setKeySpec(keySpec);
        generateRequest2.setNumberOfBytes(numberOfBytes);
    }

    // Must not flag
    private MyGenerateDataKeyRequest wrongClass(String keySpec, Integer numberOfBytes) {
        MyGenerateDataKeyRequest generateRequest = new MyGenerateDataKeyRequest()
                .withKeySpec(keySpec)
                .withNumberOfBytes(numberOfBytes);
        return generateRequest;
    }

    // Must not flag
    private GenerateDataKeyRequest conditional(String keySpec, Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest = new GenerateDataKeyRequest();
        if (numberOfBytes == 32) {
            generateRequest.setKeySpec(keySpec);
        } else {
            generateRequest.setNumberOfBytes(numberOfBytes);
        }
        return generateRequest;
    }

    // Must not flag
    private GenerateDataKeyRequest oneIsNull(Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest = new GenerateDataKeyRequest();
        generateRequest.setKeySpec(null);
        generateRequest.setNumberOfBytes(numberOfBytes);
        return generateRequest;
    }

    // Must not flag
    private GenerateDataKeyRequest differentObjects(String keySpec, Integer numberOfBytes) {
        GenerateDataKeyRequest generateRequest1 = new GenerateDataKeyRequest();
        generateRequest.setKeySpec(keySpec);
        GenerateDataKeyRequest generateRequest2 = new GenerateDataKeyRequest();
        generateRequest2.setNumberOfBytes(numberOfBytes);
        return generateRequest1;
    }
}

package example;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kms.model.GenerateDataKeyRequest;

public class KmsKeyToBytesTestCases {

    public void kmsKeyBytes() {
        // Should flag - common length
        new GenerateDataKeyRequest().withNumberOfBytes(16);

        // Should flag - common length
        // TODO: currently it isn't smart enough to flag it
        new GenerateDataKeyRequest().withNumberOfBytes(256 / 8);

        // Should not flag - not common length
        new GenerateDataKeyRequest().withNumberOfBytes(123);

        // Should not flag - different object
        new Foo().withNumberOfBytes(123);
    }
}

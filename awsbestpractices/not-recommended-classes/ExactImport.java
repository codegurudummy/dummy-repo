package example;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;

class ExactImport {
    void staticCredentialsExactImport() {
        System.out.println(new BasicAWSCredentials("my-access-key", "my-secret-key"));
    }

    void noStaticCredentials() {
        System.out.println(new BasicSessionCredentials("my-access-key", "my-secret-key", "my-session-token"));
    }
}
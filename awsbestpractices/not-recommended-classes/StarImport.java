package example;

import com.amazonaws.auth.*;

class Start {
    void staticCredentialsStarImport() {
        System.out.println(new BasicAWSCredentials("my-access-key", "my-secret-key"));
    }
}
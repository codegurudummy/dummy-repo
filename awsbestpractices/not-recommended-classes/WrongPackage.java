package example;

import example.foo.BasicAWSCredentials;

class WrongPackage {
    void staticCredentialsWrongPackage() {
        System.out.println(new BasicAWSCredentials("my-access-key", "my-secret-key"));
    }
}
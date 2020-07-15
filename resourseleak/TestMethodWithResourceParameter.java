package resourceleak;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;


public class TestMethodWithResourceParameter {

    private static String testMethodWithResourceParameter(InputStream inputStream) throws IOException {

        ByteArrayOutputStream result = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length;
        while ((length = inputStream.read(buffer)) != -1) {
            result.write(buffer, 0, length);
        }

        return result.toString(StandardCharsets.UTF_8.name());
    }

}

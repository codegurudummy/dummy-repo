package resourceleak;

import java.io.FileOutputStream;
import java.io.PrintStream;


public class TestCleanupAnnotation {

    public static void testCleanupAnnotation(String args[])throws IOException {
        String indexFile = args[1];
        @Cleanup PrintStream printStream = new PrintStream(new FileOutputStream(indexFile));
        printStream.println("<html><body>This worked</body></html>");
    }
}

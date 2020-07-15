package resourceleak;

import java.io.FileOutputStream;
import java.io.PrintStream;


public class TestCleanupAnnotationBuggy {

    public static void testCleanupAnnotationBuggy(String args[])throws IOException {
        String indexFile = args[1];
        PrintStream printStream = new PrintStream(new FileOutputStream(indexFile));
        printStream.println("<html><body>This worked</body></html>");
    }
}

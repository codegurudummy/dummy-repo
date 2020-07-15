package resourceleak;


import java.nio.channels.FileChannel;

public class TestInterpretCastExpression {

    private static void testInterpretCastExpression(Path dataFile, long newSize) throws IOException {
        try (FileChannel dataChannel = (FileChannel) Files.newByteChannel(dataFile,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC)) {
            dataChannel.truncate(newSize);
        }
    }

}

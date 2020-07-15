package resourceleak;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;


public class TestInferResourceNodeFromUsagesOfClose {

    private List<String> testInferResourceNodeFromUsagesOfClose(final String discoveryEndpoint, final String cluster) {
        if (!cachingAvailable) {
            return null;
        }

        File file = getSavedEndpointsFile(discoveryEndpoint, cluster);
        FileChannel fc = null;
        FileLock lock = null;
        String[] arreps = null;
        ByteBuffer buf = ByteBuffer.allocate(1024);
        try {
            StringBuilder s = new StringBuilder();
            fc = new FileInputStream(file).getChannel();
            lock = fc.tryLock(0L, Long.MAX_VALUE, true);
            if (lock != null) {
                int read = fc.read(buf);
                while (read != -1) {
                    buf.flip();
                    byte[] temp = new byte[read];
                    buf.get(temp);
                    s.append(new String(temp));
                    read = fc.read(buf);
                }
            }
            String data = s.toString();
            if (data != null) {
                arreps = data.split(",");
            }

        } catch (FileNotFoundException e) {
            LOGGER.log(Level.WARNING, "Unable to open " + file.getName() + " as it does not exist");
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error reading file " + file.getName() + " due to IOException");
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "General exception reading file " + file.getName() + " due to: "
                    + e.getMessage());
        } finally {
            try {
                if (fc != null) {
                    fc.close();
                }
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing file " + file.getName() + " due to IOException");
            }
        }

        if (arreps != null) {
            return new ArrayList<String>(Arrays.asList(arreps));
        } else {
            return null;
        }
    }
}

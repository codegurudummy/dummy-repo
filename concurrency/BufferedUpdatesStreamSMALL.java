package concurrency;

import org.apache.lucene.util.Accountable;

import java.io.IOException;

class BufferedUpdatesStream implements Accountable {
    public ApplyDeletesResult closeSegmentStates(IndexWriter.ReaderPool pool, SegmentState[] segStates, boolean success) throws IOException {
        if (infoStream.isEnabled("BD")) {
          infoStream.message("BD", "closeSegmentStates: " + totDelCount + " new deleted documents; pool " + updates.size() + " packets; bytesUsed=" + pool.ramBytesUsed());
        }
        return null;
    }
}
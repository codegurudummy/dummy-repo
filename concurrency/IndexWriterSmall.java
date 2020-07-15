/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package concurrency;

import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ThreadInterruptedException;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

/**
  An <code>IndexWriter</code> creates and maintains an index.

  <p>The {@link OpenMode} option on 
  {@link IndexWriterConfig#setOpenMode(OpenMode)} determines 
  whether a new index is created, or whether an existing index is
  opened. Note that you can open an index with {@link OpenMode#CREATE}
  even while readers are using the index. The old readers will 
  continue to search the "point in time" snapshot they had opened, 
  and won't see the newly created index until they re-open. If 
  {@link OpenMode#CREATE_OR_APPEND} is used IndexWriter will create a
  new index if there is not already an index at the provided path
  and otherwise open the existing index.</p>

  <p>In either case, documents are added with {@link #addDocument(Iterable)
  addDocument} and removed with {@link #deleteDocuments(Term...)} or {@link
  #deleteDocuments(Query...)}. A document can be updated with {@link
  #updateDocument(Term, Iterable) updateDocument} (which just deletes
  and then adds the entire document). When finished adding, deleting 
  and updating documents, {@link #close() close} should be called.</p>

  <a name="sequence_numbers"></a>
  <p>Each method that changes the index returns a {@code long} sequence number, which
  expresses the effective order in which each change was applied.
  {@link #commit} also returns a sequence number, describing which
  changes are in the commit point and which are not.  Sequence numbers
  are transient (not saved into the index in any way) and only valid
  within a single {@code IndexWriter} instance.</p>

  <a name="flush"></a>
  <p>These changes are buffered in memory and periodically
  flushed to the {@link Directory} (during the above method
  calls). A flush is triggered when there are enough added documents
  since the last flush. Flushing is triggered either by RAM usage of the
  documents (see {@link IndexWriterConfig#setRAMBufferSizeMB}) or the
  number of added documents (see {@link IndexWriterConfig#setMaxBufferedDocs(int)}).
  The default is to flush when RAM usage hits
  {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} MB. For
  best indexing speed you should flush by RAM usage with a
  large RAM buffer.
  In contrast to the other flush options {@link IndexWriterConfig#setRAMBufferSizeMB} and 
  {@link IndexWriterConfig#setMaxBufferedDocs(int)}, deleted terms
  won't trigger a segment flush. Note that flushing just moves the
  internal buffered state in IndexWriter into the index, but
  these changes are not visible to IndexReader until either
  {@link #commit()} or {@link #close} is called.  A flush may
  also trigger one or more segment merges which by default
  run with a background thread so as not to block the
  addDocument calls (see <a href="#mergePolicy">below</a>
  for changing the {@link MergeScheduler}).</p>

  <p>Opening an <code>IndexWriter</code> creates a lock file for the directory in use. Trying to open
  another <code>IndexWriter</code> on the same directory will lead to a
  {@link LockObtainFailedException}.</p>
  
  <a name="deletionPolicy"></a>
  <p>Expert: <code>IndexWriter</code> allows an optional
  {@link IndexDeletionPolicy} implementation to be specified.  You
  can use this to control when prior commits are deleted from
  the index.  The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
  which removes all prior commits as soon as a new commit is
  done.  Creating your own policy can allow you to explicitly
  keep previous "point in time" commits alive in the index for
  some time, either because this is useful for your application,
  or to give readers enough time to refresh to the new commit
  without having the old commit deleted out from under them.
  The latter is necessary when multiple computers take turns opening
  their own {@code IndexWriter} and {@code IndexReader}s
  against a single shared index mounted via remote filesystems
  like NFS which do not support "delete on last close" semantics.
  A single computer accessing an index via NFS is fine with the
  default deletion policy since NFS clients emulate "delete on
  last close" locally.  That said, accessing an index via NFS
  will likely result in poor performance compared to a local IO
  device. </p>

  <a name="mergePolicy"></a> <p>Expert:
  <code>IndexWriter</code> allows you to separately change
  the {@link MergePolicy} and the {@link MergeScheduler}.
  The {@link MergePolicy} is invoked whenever there are
  changes to the segments in the index.  Its role is to
  select which merges to do, if any, and return a {@link
  MergePolicy.MergeSpecification} describing the merges.
  The default is {@link LogByteSizeMergePolicy}.  Then, the {@link
  MergeScheduler} is invoked with the requested merges and
  it decides when and how to run the merges.  The default is
  {@link ConcurrentMergeScheduler}. </p>

  <a name="OOME"></a><p><b>NOTE</b>: if you hit a
  VirtualMachineError, or disaster strikes during a checkpoint
  then IndexWriter will close itself.  This is a
  defensive measure in case any internal state (buffered
  documents, deletions, reference counts) were corrupted.  
  Any subsequent calls will throw an AlreadyClosedException.</p>

  <a name="thread-safety"></a><p><b>NOTE</b>: {@link
  IndexWriter} instances are completely thread
  safe, meaning multiple threads can call any of its
  methods, concurrently.  If your application requires
  external synchronization, you should <b>not</b>
  synchronize on the <code>IndexWriter</code> instance as
  this may cause deadlock; use your own (non-Lucene) objects
  instead. </p>
  
  <p><b>NOTE</b>: If you call
  <code>Thread.interrupt()</code> on a thread that's within
  IndexWriter, IndexWriter will try to catch this (eg, if
  it's in a wait() or Thread.sleep()), and will then throw
  the unchecked exception {@link ThreadInterruptedException}
  and <b>clear</b> the interrupt status on the thread.</p>
*/

/*
 * Clarification: Check Points (and commits)
 * IndexWriter writes new index files to the directory without writing a new segments_N
 * file which references these new files. It also means that the state of
 * the in memory SegmentInfos object is different than the most recent
 * segments_N file written to the directory.
 *
 * Each time the SegmentInfos is changed, and matches the (possibly
 * modified) directory files, we have a new "check point".
 * If the modified/new SegmentInfos is written to disk - as a new
 * (generation of) segments_N file - this check point is also an
 * IndexCommit.
 *
 * A new checkpoint always replaces the previous checkpoint and
 * becomes the new "front" of the index. This allows the IndexFileDeleter
 * to delete files that are referenced only by stale checkpoints.
 * (files that were created since the last commit, but are no longer
 * referenced by the "front" of the index). For this, IndexFileDeleter
 * keeps track of the last non commit checkpoint.
 */
public class IndexWriter implements Closeable, TwoPhaseCommit, Accountable {


  /** Holds shared SegmentReader instances. IndexWriter uses
   *  SegmentReaders for 1) applying deletes/DV updates, 2) doing
   *  merges, 3) handing out a real-time reader.  This pool
   *  reuses instances of the SegmentReaders in all these
   *  places if it is in "near real-time mode" (getReader()
   *  has been called on this instance). */

  class ReaderPool implements Closeable {
    

    public synchronized boolean anyChanges() {
      for (ReadersAndUpdates rld : readerMap.values()) {
        // NOTE: we don't check for pending deletes because deletes carry over in RAM to NRT readers
        if (rld.getNumDVUpdates() != 0) {
          return true;
        }
      }

      return false;
    }

    /**
     * Obtain a ReadersAndLiveDocs instance from the
     * readerPool.  If create is true, you must later call
     * {@link #release(ReadersAndUpdates)}.
     */
    public synchronized ReadersAndUpdates get(SegmentCommitInfo info, boolean create) {

      // Make sure no new readers can be opened if another thread just closed us:
      ensureOpen(false);
      anyChanges();
      
       assert info.info.dir == directoryOrig: "info.dir=" + info.info.dir + " vs " + directoryOrig;

      ReadersAndUpdates rld = readerMap.get(info);
      if (rld == null) {
        if (create == false) {
          return null;
        }
        rld = new ReadersAndUpdates(IndexWriter.this, info);
        // Steal initial reference:
        readerMap.put(info, rld);
      } else {
        assert rld.info == info: "rld.info=" + rld.info + " info=" + info + " isLive?=" + assertInfoIsLive(rld.info) + " vs " + assertInfoIsLive(info);
      }

      if (create) {
        // Return ref to caller:
        rld.incRef();
      }
      noDups();
//      assert noDups();

      return rld;
    }

    // Make sure that every segment appears only once in the
    // pool:
    private boolean noDups() {
      Set<String> seen = new HashSet<>();
      for(SegmentCommitInfo info : readerMap.keySet()) {
        assert !seen.contains(info.info.name);
        seen.add(info.info.name);
      }
      return true;
    }
  }

}
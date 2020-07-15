package concurrency;

import com.amazon.bigbird.dataaccess.AttributeName;
import com.amazon.bigbird.dataaccess.AttributeValue;
import com.amazon.bigbird.dataaccess.IndexKey;
import com.amazon.bigbird.dataaccess.Item;
import com.amazon.bigbird.indexmanagerWT.WTHelper;
import com.amazon.bigbird.indexmanagerWT.wiredtigerWrapper.WiredTigerWrapper;
import com.amazon.bigbird.logging.BigBirdFatalInterceptor;
import com.amazon.bigbird.ondiskobjects.IndexManagerRowKey;
import com.amazon.bigbird.ondiskobjects.OnDiskAttributeMapV2;
import com.amazon.bigbird.ondiskobjects.UpdateLogEntry;
import com.amazon.bigbird.storageengine.StorageInstanceManager.StorageInstance;
import com.amazon.bigbird.storagenode.StorageNodeException;
import com.amazon.bigbird.wiredtiger.WiredTigerException;
import s3.commons.log.S3Logger;
import s3.commons.util.Pair;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.amazon.bigbird.exceptions.BigBirdAssert.bbAssert;

/**
 * Durable store that provides immediate durability instead of the periodic checkpointing provided by default
 * WT.
 * 
 * NOTE: We have non replicated data only in metadata table. So Durable Store is a single json file
 * that holds all data. This is not an actual store (neither is supposed to be very performant), So we need to keep it
 * Simple. If this turns out to be a full on store for any reason, reevaluate this call.
 * 
 * @author ssubra
 *
 */
public class DurableLocalStoreManagerImpl implements DurableLocalStoreManager {
    private static final S3Logger logger = new BigBirdFatalInterceptor();

    protected final StorageInstanceManager sim;
    
    // bucketName -> DLS for that bucket
    private final Map<String, DurableLocalStore> bucketDataMap;

    /**
     * This checks if the files are present for apply/export
     * 
     * @param dbDir
     * @param bucketName
     * @return
     */
    public static boolean verifyStoreFilesPresent(String dbDir, String bucketName) {
        return DurableLocalJsonStore.verifyStoreFilesPresent(dbDir, bucketName);
    }
    
    public static boolean verifyIntegrity(String dbDir, String bucketName) {
        //TODO Add an implementation when we add the Verify task at NCListener side.
        return false;
    }

    /**
     * This is used to move the store files after an import - from the import location to actual.
     */
    public static void moveStoreFiles(String dbDir,
                                      String newBucketName,
                                      String fromDir,
                                      String oldBucketName) throws Exception
    {
        DurableLocalJsonStore.moveStoreFiles(dbDir, newBucketName, fromDir, oldBucketName);
    }

    public static void dropStoreFiles(String dir, String bucket) throws IOException {
        DurableLocalJsonStore.dropStoreFiles(dir, bucket);
    }

    /**
     * Factory method to allow for a quick swap between stores. protected to allow for unit test overrides.
     * 
     * @return
     */
    protected DurableLocalStore newStore(String bucketName, boolean create) throws StorageNodeException {
        return new DurableLocalJsonStore(sim, bucketName, create);
    }

    public DurableLocalStoreManagerImpl(StorageInstanceManager sim) {
        this.sim = sim;
        this.bucketDataMap = new ConcurrentHashMap<>();
    }

    @Override
    public boolean putOrUpdate(String bucketName, IndexKey key, Item value) throws StorageNodeException {
        final DurableLocalStore store = getBucketData(bucketName);
        store.assertIfNotOpen("putOrUpdate");
        return store.putOrUpdate(key, value);
    }

    @Override
    public Item get(String bucketName, IndexKey key) throws StorageNodeException {
        final DurableLocalStore store = getBucketData(bucketName);
        store.assertIfNotOpen("get");
        return store.get(key);
    }

    @Override
    public void create(String bucketName) throws StorageNodeException {
        DurableLocalStore dataStore = bucketDataMap.get(bucketName);

        if (dataStore == null) {
            synchronized (this) {
                dataStore = bucketDataMap.get(bucketName);
                if (dataStore == null) {
                    dataStore = newStore(bucketName, true);
                    bucketDataMap.put(bucketName, dataStore);
                    return;
                }
            }
        }
    }

    @Override
    public void load(String bucketName) throws StorageNodeException {
        DurableLocalStore dataStore = bucketDataMap.get(bucketName);

        if (dataStore == null) {
            synchronized (this) {
                dataStore = bucketDataMap.get(bucketName);
                if (dataStore == null) {
                    dataStore = newStore(bucketName, false);
                    bucketDataMap.put(bucketName, dataStore);
                    return;
                }
            }
        }
    }

    @Override
    public void dropUnconditionally(String bucketName) throws StorageNodeException {
        final String TRACE_HEADER = "dropUnconditionally";
        //We have to handle the following cases:
        // store is present, connection is open.
        // store is present, connection is closed.
        // store is not present.

        DurableLocalStore dataStore = bucketDataMap.get(bucketName);
        if(dataStore == null) {
            //open the store if available. 
            try {
                load(bucketName);
            } catch (IllegalStateException e) {
                //store is deleted already Or is an unknown bucket - nothing to do!
                logger.info(TRACE_HEADER,"durable store is already deleted on disk / an unknown bucket",
                            "bucket name", bucketName);
                return;
            }
            dataStore = bucketDataMap.get(bucketName);
        }
        dataStore.dropUnconditionally();
        //drop the reference
        safeClose(bucketName);
    }

    @Override
    public void safeClose(String bucketName) throws StorageNodeException {
        final DurableLocalStore dataStore = bucketDataMap.remove(bucketName);
        if(dataStore != null) {
            dataStore.safeClose();
        }
    }
    
    @Override
    public void backup(String bucketName, String outputDir) {
        final DurableLocalStore dataStore = bucketDataMap.get(bucketName);
        dataStore.assertIfNotOpen("backup");
        dataStore.backup(outputDir);
    }

    public boolean importLocalStore(String importDir, String bucketName) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support importLocalStore()");
    }

    @Override
    public DurableLocalStore exportLocalStore(String bucketName, String outputDir) throws StorageNodeException {
        backup(bucketName, outputDir);
        return new DurableLocalJsonStore(outputDir, bucketName, false);
    }

    @Override
    public DurableLocalStore getDurableLocalStore(String bucketName) {
        return bucketDataMap.get(bucketName);
    }

    @Override
    public void cleanupForUnitTestOnly(boolean doCleanupData) {
        //to allow for deletes from bucketDataMap since safeClose modifies it.
        Set<String> buckets = new HashSet<>(this.bucketDataMap.keySet());
        for(String bucketName : buckets) {
            safeClose(bucketName);
            if(doCleanupData) {
                dropSingleStoreForUnitTestOnly(bucketName);
            }
        }
        this.bucketDataMap.clear();
    }
    
    @Override
    public void dropAllStoresForUnitTestOnly() {
        for (final StorageInstance si :  sim.getDataStorageInstances()) {
            File dataDir = new File(si.instanceConfiguration.getEngineConfig().getDataDir());
            if(dataDir.exists() && dataDir.isDirectory()) {
                for(File file : dataDir.listFiles()) {
                    if(DurableLocalStore.isDurableStoreName(file.getName())) {
                        dropSingleStoreForUnitTestOnly(sim.dbPrefix, dataDir.getPath(), file.getName());
                    }
                }
            }
        }
    }

    protected void dropSingleStoreForUnitTestOnly(String bucketName) {
        final String dataFolder = sim.getStorageInstance(bucketName).instanceConfiguration
        .getEngineConfig().getDataDir();
        final String storeName = DurableLocalStore.getStoreName(bucketName);
        
        dropSingleStoreForUnitTestOnly(sim.dbPrefix, dataFolder, storeName);
    }
    
    private void dropSingleStoreForUnitTestOnly(String dbPrefix, String dataFolder, String storeName) {

        //delete only if the store / file belongs to this dbPrefix. Else we mess with parallel nodes.
        if(! storeName.startsWith(dbPrefix)) {
            return;
        }
        
        // cleanup both kinds possible here.
        try {
            WiredTigerWrapper.getInstance(dbPrefix).dropDatabase(new Pair<>(dataFolder, storeName), true);
        } catch (WiredTigerException e) {
            // eat the stack trace if this fails - test cleanup
        }

        // for json files and WT folder - generic drop
        new File(dataFolder, storeName).delete();
    }

    public static byte[] getValueByteArray(Item value) throws IOException {
        Map<AttributeName, AttributeValue> attributesToBeCompacted = UpdateLogEntry.generateAttributesToBeCompacted(
            value, LocalMetadataConstant.replicaMetadataSchema);
        return OnDiskAttributeMapV2.serialize(attributesToBeCompacted, null, null);
    }

    public static byte[] getKeyByteArray(IndexKey key) {
        return WTHelper.serializeKey(key);
    }

    public static IndexKey getIndexKey(byte[] key) {
        return WTHelper.deserializeKey(key);
    }

    public static Item getItemFromByteArray(IndexKey key, byte[] bs) {
        
        if(bs == null) {// can happen when item is not found.
            return null;
        }
        
        IndexManagerRowKey rowKey = new IndexManagerRowKey(key, 0);
        byte[][] blobChunks = new byte[1][];
        blobChunks[0] = bs;
        return new Item(UpdateLogEntry.translateCompactResultToMap(LocalMetadataConstant.replicaMetadataSchema, rowKey,
            blobChunks, null /* no keyid map for metadata */));
    }

    private DurableLocalStore getBucketData(String bucketName) throws StorageNodeException {
        DurableLocalStore store = bucketDataMap.get(bucketName);
        bbAssert(store != null, "getBucketData", "store not loaded!", "bucketName", bucketName);

        return store;
    }

    @Override
    // remember that this call returns the state of the bucket at this moment.
    // the state might get changed later. So don't write code assuming that the state will stay
    public boolean checkDurableLocalStoreExists(String bucketName) {
        return bucketDataMap.containsKey(bucketName);
    }
}
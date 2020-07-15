package concurrency;

import com.amazon.sable.constants.Headers;
import com.amazon.sable.constants.Verbs;
import com.amazon.sable.netty.context.RemoteContext;
import com.amazon.sable.netty.framing.stumpy.Message;
import com.amazon.sable.netty.framing.stumpy.MessageUtils;
import com.amazon.sable.storage.StorageDevice;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Simple in-memory instance of the SABLE storage device for test
 * 
 * <p>
 * </p>
 * 
 * Here is what's outstanding:
 * 
 * <ul>
 * <li>entity size</li>
 * <li>conditional operations are not implemented whatsoever</li>
 * <li>GETKEYPREFIX count:x (limit the number of responses)</li>
 * <li>GETKEYPREFIX keystart:y (where to start the responses)</li>
 * </ul>
 * 
 * @see <a href="https://w.amazon.com/index.php/Sable/Design/StoreProtocol">Store Protocol</a>
 * @see <a href="https://w.amazon.com/index.php/Sable/Interface">Client documentation</a>
 * @see <a href="https://internal.amazon.com/~bailes/pub/2013-sable-entity-size.html">Entity Count</a>
 */

public class SableMemoryStorageDeviceImpl implements StorageDevice {

    /**
     * Max total number of bytes within all headers
     */

    private static final int MAX_HEADER_SIZE = 1 * 1024;

    /**
     * Max number of bytes within the payload
     */

    private static final int MAX_PAYLOAD_SIZE = 256 * 1024;

    /**
     * Memory backed data store
     */

    private final Map<String, SortedMap<String, SableMemoryRecord>> delegate = new HashMap<String, SortedMap<String, SableMemoryRecord>>();

    @Override
    public boolean enqueue(RemoteContext remoteContext) {

        final String verb = remoteContext.getMessage().getVerb();

        if (StringUtils.isBlank(verb)) {

            // missing verb

            final Message rsp = new Message();

            MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

            rsp.setVerb(Verbs.ACK);

            rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
            rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_INVALID_MESSAGE);

            rsp.setPayload("Missing verb".getBytes());

            remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

        } else if (getHeaderSize(remoteContext.getMessage()) > MAX_HEADER_SIZE) {

            // headers too big

            final Message rsp = new Message();

            MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

            rsp.setVerb(Verbs.ACK);

            rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
            rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_INVALID_MESSAGE);

            rsp.setPayload("Headers too big".getBytes());

            remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

        } else {

            if (StringUtils.equals(Verbs.PUBLISH, verb) || StringUtils.equals(Verbs.GETKEY, verb)) {

                // storage operation (put/get/delete)

                final String key = remoteContext.getMessage().getHeaderMap().get(Headers.KEY);

                if (StringUtils.isBlank(key)) {

                    // key is required for storage operations

                    Message rsp = new Message();

                    MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                    rsp.setVerb(Verbs.ACK);

                    rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                    rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_MISSING_KEY);

                    rsp.setPayload(("Verb \"" + verb + "\" requires the header \"key\"").getBytes());

                    remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

                } else {

                    final String entity;
                    final String discriminator;

                    int index = key.indexOf(":");

                    if (index == -1) {
                        entity = key;
                        discriminator = "";
                    } else {
                        entity = key.substring(0, index);
                        discriminator = key.substring(index);
                    }
                    if (StringUtils.equals(Verbs.PUBLISH, verb)) {

                        // PUBLISH could be put or delete

                        final String operation = remoteContext.getMessage().getHeaderMap().get(Headers.OPERATION);

                        if (StringUtils.isBlank(operation)) {

                            // operation required for PUBLISH

                            final Message rsp = new Message();

                            MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                            rsp.setVerb(Verbs.ACK);

                            rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                            rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_MISSING_OPERATION);

                            rsp.setPayload("Verb \"PUBLISH\" requires the header \"operation\"".getBytes());

                            remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

                        } else if (StringUtils.equals(operation, Headers.PUT)) {

                            // this is a PUT

                            final String scope = remoteContext.getMessage().getHeaderMap().get(Headers.SCOPE);
                            final String time = remoteContext.getMessage().getHeaderMap().get(Headers.TIME);
                            final String expires = remoteContext.getMessage().getHeaderMap().get(Headers.EXPIRES);
                            final byte[] payload = remoteContext.getMessage().getPayload();

                            if (payload.length > MAX_PAYLOAD_SIZE) {

                                // payload too big

                                final Message rsp = new Message();

                                MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                                rsp.setVerb(Verbs.ACK);

                                rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                                rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_INVALID_MESSAGE);

                                rsp.setPayload(("Payload must be less than " + MAX_PAYLOAD_SIZE + " bytes").getBytes());

                                remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

                            } else {

                                SableMemoryRecord.Builder builder = new SableMemoryRecord.Builder();

                                builder.setOperation(operation).setScope(scope).setKey(key).setTime(time)
                                        .setExpires(expires).setPayload(payload);

                                SableMemoryRecord sableMemoryRecord = builder.build();

                                int entityCount = 0;

                                synchronized (delegate) {

                                    SortedMap<String, SableMemoryRecord> node = delegate.get(entity);

                                    if (node == null) {
                                        node = new TreeMap<String, SableMemoryRecord>();
                                        delegate.put(entity, node);
                                    }
                                    node.put(discriminator, sableMemoryRecord);

                                    entityCount = node.size();
                                }

                                final Message rsp = new Message();

                                MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                                rsp.setVerb(Verbs.ACK);

                                rsp.getHeaderMap().put(Headers.STATUS, Headers.OK);
                                rsp.getHeaderMap().put(Headers.URID, sableMemoryRecord.getUrid());
                                rsp.getHeaderMap().put(Headers.ENTITY_COUNT, Integer.toString(entityCount));

                                remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);
                            }

                        } else if (StringUtils.equals(operation, Headers.REMOVE)) {

                            // this is a delete

                            int entityCount = 0;

                            synchronized (delegate) {

                                Map<String, SableMemoryRecord> node = delegate.get(entity);

                                if (node != null) {

                                    node.remove(discriminator);

                                    entityCount = node.size();

                                    if (node.isEmpty()) {
                                        delegate.remove(entity); // remove empty map
                                    }
                                }
                            }

                            final Message rsp = new Message();

                            MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                            rsp.setVerb(Verbs.ACK);

                            rsp.getHeaderMap().put(Headers.STATUS, Headers.OK);
                            rsp.getHeaderMap().put(Headers.ENTITY_COUNT, Integer.toString(entityCount));

                            remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

                        } else {

                            // unknown operation

                            final Message rsp = new Message();

                            MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                            rsp.setVerb(Verbs.ACK);

                            rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                            rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_INVALID_OPERATION);

                            rsp.setPayload(("Verb \"PUBLISH\" with operation \"" + operation + "\" is invalid")
                                    .getBytes());

                            remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);
                        }

                    } else {

                        // this is a GET

                        final boolean isNodata = isNodata(remoteContext.getMessage());
                        int count = 0;
                        int entityCount = 0;

                        synchronized (delegate) {

                            Map<String, SableMemoryRecord> node = delegate.get(entity);

                            if (node != null) {

                                SableMemoryRecord sableMemoryRecord = node.get(discriminator);

                                if (sableMemoryRecord != null) {

                                    Message rsp = transformRecordToMessage(remoteContext, isNodata, sableMemoryRecord);

                                    ++count;

                                    remoteContext.getMessageCallback()
                                            .onMessage(remoteContext.getRequestContext(), rsp);

                                }
                                entityCount = node.size();
                            }
                        }

                        final Message rsp = new Message();

                        MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                        rsp.setVerb(Verbs.ACK);

                        rsp.getHeaderMap().put(Headers.STATUS, Headers.OK);
                        rsp.getHeaderMap().put(Headers.COUNT, Integer.toString(count));
                        rsp.getHeaderMap().put(Headers.ENTITY_COUNT, Integer.toString(entityCount));

                        remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);
                    }
                }

            } else if (StringUtils.equals(Verbs.GETKEYPREFIX, verb)) {

                // this is a list

                final String keyprefix = remoteContext.getMessage().getHeaderMap().get(Headers.KEYPREFIX);

                if (StringUtils.isBlank(keyprefix)) {

                    final Message rsp = new Message();

                    MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                    rsp.setVerb(Verbs.ACK);

                    rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                    rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_MISSING_KEYPREFIX);

                    rsp.setPayload("Verb \"GETKEYPREFIX\" requires the header \"keyprefix\"".getBytes());

                    remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);

                } else {

                    final boolean isNodata = isNodata(remoteContext.getMessage());

                    final String entity;
                    final String discriminator;

                    int index = keyprefix.indexOf(":");

                    if (index == -1) {
                        entity = keyprefix;
                        discriminator = "";
                    } else {
                        entity = keyprefix.substring(0, index);
                        discriminator = keyprefix.substring(index);
                    }

                    int count = 0;
                    int entityCount = 0;

                    synchronized (delegate) {

                        Map<String, SableMemoryRecord> node = delegate.get(entity);

                        if (node != null) {

                            for (Map.Entry<String, SableMemoryRecord> entry : node.entrySet()) {

                                if (entry.getKey().startsWith(discriminator)) {

                                    SableMemoryRecord sableMemoryRecord = entry.getValue();

                                    Message rsp = transformRecordToMessage(remoteContext, isNodata, sableMemoryRecord);

                                    rsp.getHeaderMap().put(Headers.INDEX, Integer.toString(count));

                                    ++count;

                                    remoteContext.getMessageCallback()
                                            .onMessage(remoteContext.getRequestContext(), rsp);
                                }
                            }
                            entityCount = node.size();
                        }
                    }

                    final Message rsp = new Message();

                    MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                    rsp.setVerb(Verbs.ACK);

                    rsp.getHeaderMap().put(Headers.STATUS, Headers.OK);
                    rsp.getHeaderMap().put(Headers.COUNT, Integer.toString(count));
                    rsp.getHeaderMap().put(Headers.ENTITY_COUNT, Integer.toString(entityCount));

                    remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);
                }

            } else {

                // no clue what this verb is

                final Message rsp = new Message();

                MessageUtils.copyMsid(remoteContext.getMessage(), rsp);

                rsp.setVerb(Verbs.ACK);

                rsp.getHeaderMap().put(Headers.STATUS, Headers.ERROR);
                rsp.getHeaderMap().put(Headers.REASON, Headers.REASON_INVALID_MESSAGE);

                rsp.setPayload(("Verb \"" + verb + "\" is unknown").getBytes());

                remoteContext.getMessageCallback().onMessage(remoteContext.getRequestContext(), rsp);
            }
        }

        boolean isFlushedToStorageDevice = true;

        return isFlushedToStorageDevice;
    }

    @Override
    public void shutdown() {
        delegate.clear();
    }

    private static int getHeaderSize(Message message) {

        int retval = 0;

        for (Map.Entry<String, String> entry : message.getHeaderMap().entrySet()) {

            retval += entry.getKey().length();
            retval += entry.getValue().length();
        }

        return retval;
    }

    private static Message transformRecordToMessage(RemoteContext remoteContext, boolean isNodata,
            SableMemoryRecord sableMemoryRecord) {

        Message retval = new Message();

        MessageUtils.copyMsid(remoteContext.getMessage(), retval);

        retval.setVerb(Verbs.GETR);

        retval.getHeaderMap().put(Headers.SCOPE, sableMemoryRecord.getScope());
        retval.getHeaderMap().put(Headers.KEY, sableMemoryRecord.getKey());
        retval.getHeaderMap().put(Headers.OPERATION, sableMemoryRecord.getOperation());
        retval.getHeaderMap().put(Headers.TIME, sableMemoryRecord.getTime());
        retval.getHeaderMap().put(Headers.URID, sableMemoryRecord.getUrid());

        if (StringUtils.isNotBlank(sableMemoryRecord.getExpires())) {
            retval.getHeaderMap().put(Headers.EXPIRES, sableMemoryRecord.getExpires());
        }
        if (!isNodata) {
            retval.setPayload(sableMemoryRecord.getPayload());
        }
        return retval;
    }

    private static boolean isNodata(Message message) {

        boolean retval = false;

        String nodata = message.getHeaderMap().get(Headers.NODATA);

        if (StringUtils.equals(nodata, Headers.TRUE)) {

            retval = true;
        }
        return retval;
    }

    @Override
    public StorageDeviceType getType() {
        return StorageDeviceType.SABLE_ROUTER;
    }

}
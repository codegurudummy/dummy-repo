package resourceleak;

import java.sql.Connection;
import java.sql.SQLException;

public class TestConnectionResourcesNotClosed {

    public Long testConnectionResourcesNotClosed(BranchType branchType, String resourceId, String clientId,
                                                 String xid, String lockKey) throws TransactionException {

        Integer callSeq = MetaDataFilter.getMetaData(EasytransConstant.CallHeadKeys.CALL_SEQ);

        // check locks
        if (StringUtils.isNullOrEmpty(lockKey)) {
            return callSeq== null?-1:callSeq.longValue();
        }

        //ET要求使用Spring管控下的事务，因此本方法可以获得对应的当前连接,获取当前连接来执行时为了避免争夺连接词的连接导致死锁
        DataSourceProxy dataSourceProxy = get(resourceId);
        ConnectionProxy cp = (ConnectionProxy) DataSourceUtils.getConnection(dataSourceProxy);
        Connection targetConnection = cp.getTargetConnection();

        if (callSeq != null) {
            // callSeq != null means it's in ET transaction control
            try {
                doLockKeys(xid, lockKey, targetConnection);
            } catch (SQLException e) {
                throw new RuntimeException("Obtain Lock failed, Rollback transaction：" + lockKey, e);
            }

        } else {
            // callSeq == null means it's just a local transaction or a master transaction in ET
            // it need to check lock
            if(!lockQuery(branchType, resourceId, xid, lockKey)) {
                throw new RuntimeException("Obtain Lock failed, Rollback transaction：" + lockKey);
            }

            // not need to save undolog ,undo will be handle by local transaction, just hack to clean undo log
            cp.getContext().getUndoItems().clear();
        }

        return callSeq== null?-1:callSeq.longValue();
    }
}

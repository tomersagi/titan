package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import org.mapdb.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapdBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(MapdBTx.class);
    private DB tx = null;

    public MapdBTx(BaseTransactionConfig config, MapDBKeyValueStore store) {
        super(config);
        tx = store.getManager().getTxMaker().makeTx();
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null)
            log.error("Rollback attempted on null mapDB tx");
        tx.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
            tx = null;

    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        if (tx == null || tx.isClosed()) return;
        try {
            tx.commit();
        } catch (java.lang.IllegalAccessError e) {
            log.error("closed...");
            throw new PermanentBackendException(e);
        } catch (org.mapdb.TxRollbackException e) {
            //Transaction failed, other transaction dirtied our data, need to restart
            throw new TemporaryBackendException(e);
        }
        //log.debug("Committed");
        if (log.isTraceEnabled())
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    public DB getTx() {
        return tx;
    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}

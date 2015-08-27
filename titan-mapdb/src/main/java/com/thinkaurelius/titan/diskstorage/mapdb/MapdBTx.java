package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import org.mapdb.DB;
import org.mapdb.TxBlock;
import org.mapdb.TxRollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MapdBTx extends AbstractStoreTransaction {

    private static final StaticBuffer.Factory<DBEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DBEntry>() {
        @Override
        public DBEntry get(byte[] array, int offset, int limit) {
            return new DBEntry(array,offset,limit-offset);
        }
    };
    private class TxAction {
        public char action='i'; //change to d for delete
        public String mName;
        public StaticBuffer key=null;
        public StaticBuffer value=null;

        /**
         * constructor for delete action
         * @param name of the store from which to delete a key
         * @param key to delete
         *
         */
        public TxAction(String name, StaticBuffer key) {
            this.mName = name;
            this.action = 'd';
            this.key = key;
        }

        /**
         * Constructor for insert actions
         * @param name of store in which to insert
         * @param key to update
         * @param value to insert
         */
        public TxAction(String name,StaticBuffer key, StaticBuffer value) {
            this.mName = name;
            this.key = key;
            this.value = value;
        }
    }

    private List<TxAction> insertLog=null;
    private static final Logger log = LoggerFactory.getLogger(MapdBTx.class);
    private DB tx = null;
    private final MapDBStoreManager mgr;
    private boolean read=false;
    private boolean write=false;

    public MapdBTx(BaseTransactionConfig config, DB tx, MapDBStoreManager mgr) {
        super(config);
        this.mgr =mgr;
        this.tx = tx;
        log.debug("New MapDB tx {}", tx);
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        if (tx == null)
            log.error("Rollback attempted on null mapDB tx");
        tx.rollback();
        log.debug("tx {} rolled back",this.tx);
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
            tx = null;

    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        String txid=tx.toString();
        if (tx == null || tx.isClosed()) return;
            log.debug("Trying to commit tx {}", tx);
        if (write && !read) { //These are replayed again and again on mapDB until success
            try {
                tx.commit(); //If at first you dont succeed
            }
            catch (TxRollbackException txRB) {
                //try try again
                TxBlock txb = new TxBlock() {
                    @Override
                    public void tx(DB db) throws TxRollbackException {
                        DB myTx = mgr.getTxMaker().makeTx();
                        for (TxAction txa : insertLog) {
                            if (txa.action=='i')
                                myTx.treeMap(txa.mName, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).put(txa.key.as(ENTRY_FACTORY), txa.value.as(ENTRY_FACTORY));
                            else //delete
                                tx.treeMap(txa.mName, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).remove(txa.key.as(ENTRY_FACTORY));


                        }
                        myTx.commit();
                    }
                };
                mgr.getTxMaker().execute(txb);
            }
            finally {
                if (!tx.isClosed())
                    tx.rollback();
            }

            } else // read only will work,  both read and write in a transaction cannot be recorded by the adapter, hope it works...
                try {
                        tx.commit();
        } catch (java.lang.IllegalAccessError e) {
            log.error("closed...");
            throw new PermanentBackendException(e);
        } catch (org.mapdb.TxRollbackException e) {
            //Transaction failed, other transaction dirtied our data, need to restart
            throw new TemporaryBackendException(e);
        }
        log.debug("Committed tx {}",txid);
        if (log.isTraceEnabled())
            log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == tx ? "nulltx" : tx.toString());
    }

    public void insert(String mapName, StaticBuffer key, StaticBuffer value) {
        write=true;
        tx.treeMap(mapName, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).put(key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
        if (this.insertLog==null)
            insertLog = new ArrayList<TxAction>();
        insertLog.add(new TxAction(mapName,key,value));
    }

    public StaticBuffer get(String mapName, StaticBuffer key) {
        read=true;
        log.debug(" tx {} read key {}", tx, key.as(ENTRY_FACTORY).toString());
        return MapDBKeyValueStore.getBuffer(tx.treeMap(mapName, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).get(key.as(ENTRY_FACTORY)));
    }

    public void delete(String mapName, StaticBuffer key) {
        write=true;
        if (this.insertLog==null)
            insertLog = new ArrayList<TxAction>();
        insertLog.add(new TxAction(mapName,key));
        tx.treeMap(mapName, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).remove(key.as(ENTRY_FACTORY));
        log.debug(" tx {} deleted key {}", tx, key.as(ENTRY_FACTORY).toString());

    }

    public DB getSlice() {
        read = true;
        return tx;
    }


    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }
}

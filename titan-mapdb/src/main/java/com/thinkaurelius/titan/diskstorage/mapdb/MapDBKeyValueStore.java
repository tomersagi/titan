package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class MapDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(MapDBKeyValueStore.class);

    private static final StaticBuffer.Factory<DBEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DBEntry>() {
        @Override
        public DBEntry get(byte[] array, int offset, int limit) {
            return new DBEntry(array,offset,limit-offset);
        }
    };

    private static StaticBuffer getBuffer(DBEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }

    private final String name;
    private final MapDBStoreManager manager;
    private boolean isOpen;


    /**
     * Creates a file backed MapDB where the file path will be dir / storeName.mapdb
     *
     * @param n   name of this store
     * @param m   Store manager handles multiple store management
     */
    public MapDBKeyValueStore(String n, MapDBStoreManager m) {
        name = n;
        manager = m;

        DB tmpDB=null;

        try {
            tmpDB = m.getTxMaker().makeTx();
            if (!tmpDB.exists(name))
                tmpDB.treeMapCreate(name).keySerializer(MapDBStoreManager.DBKSER).valueSerializer(MapDBStoreManager.DBSER).make();
//            tmpDB.commit(); moved to finally

        } catch (IllegalArgumentException e) {
            //noop

        } finally {
            if (tmpDB!=null)
                tmpDB.commit();
        }
        isOpen = true;
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public synchronized void close() throws BackendException {
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
        boolean noTX = (txh == null);
        if (noTX)
            throw new TemporaryBackendException("txh is null!");
        DB tx = ((MapdBTx) txh).getTx();
        Map<Object, DBEntry> map = tx.treeMap(name, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER);
        return getBuffer(map.get(key.as(ENTRY_FACTORY)));
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
//        if (getTransaction(txh) == null) {
//            log.warn("Attempt to acquire lock with transactions disabled");
//        } //else we need no locking
        //noop
    }

    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final MapdBTx mTxH = (MapdBTx) txh;
        if (mTxH.getTx().isClosed())
            log.error("Requested read from closed database");
        return new RecordIterator<KeyValueEntry>() {
            DB tx = mTxH.getTx();
            BTreeMap<Object, DBEntry> map = tx.treeMap(name, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER);

            private final Iterator<Map.Entry<Object, DBEntry>> entries = map.subMap(keyStart.as(ENTRY_FACTORY), keyEnd.as(ENTRY_FACTORY)).entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    Map.Entry<Object, DBEntry> ent = entries.next();
                    return new KeyValueEntry(getBuffer((DBEntry)ent.getKey()), getBuffer(ent.getValue()));
                }

                @Override
                public void close() {
                    //tx.rollback();
                    //tx.close();
                }

            @Override
            public void remove()  {
                    throw new UnsupportedOperationException();
                }
        };

    }


    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        boolean noTX = (txh == null);
        if (noTX)
            throw new TemporaryBackendException("txh is null!");
        DB tx = ((MapdBTx) txh).getTx();
        tx.treeMap(name, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).put(key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) {
        boolean noTX = (txh == null);
        if (noTX)
            return;
        DB tx = ((MapdBTx) txh).getTx();
        tx.treeMap(name, MapDBStoreManager.DBKSER, MapDBStoreManager.DBSER).remove(key.as(ENTRY_FACTORY));
    }

    public void clear() {
        DB tx = manager.getTxMaker().makeTx();
        tx.treeMap(name).clear();
        tx.commit();
    }

    public MapDBStoreManager getManager() {
        return manager;
    }
}

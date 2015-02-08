/*
 Copyright 2015 Hewlett-Packard Development Company, L.P.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * a MapDBKeyValueStore corresponds to a MapDB TreeMap
 */
public class MapDBKeyValueStore implements OrderedKeyValueStore {

//    private static final Logger log = LoggerFactory.getLogger(MapDBKeyValueStore.class);

    private static final StaticBuffer.Factory<DBEntry> ENTRY_FACTORY = new StaticBuffer.Factory<DBEntry>() {
        @Override
        public DBEntry get(byte[] array, int offset, int limit) {
            return new DBEntry(array,offset,limit-offset);
        }
    };

    private String name;
    private final MapDBStoreManager manager;

    /**
     * Creates a map in this MapDB with the given name
     *
     * @param n   name of this store
     * @param m   Store manager handles multiple store management
     */
    public MapDBKeyValueStore(String n, MapDBStoreManager m) {
        name = n;
        manager = m;
//        DB mapMakeTx = manager.getTxMaker().makeTx();
//        Map<DBEntry, DBEntry> map = mapMakeTx.getTreeMap(n);
//        mapMakeTx.commit();
        //noop MapDB creates maps when first accessed
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public synchronized void close() throws BackendException {
        //noop
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {
//        boolean noTX = (txh == null);
//        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        DB tx = ((MapdBTx) txh).getTx();
        Map<DBEntry, DBEntry> map = tx.getTreeMap(name);
        return  new StaticArrayBuffer(getBuffer(map.get(key.as(ENTRY_FACTORY))));
//        if (noTX)
//            tx.commit();
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
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, final StoreTransaction txh) throws BackendException {
        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
//        final boolean noTX = (txh == null);
//        final MapdBTx mTxH =  txh);
        return new RecordIterator<KeyValueEntry>() {
//            DB tx = (noTX ? txMaker.makeTx() : mTxH.getTx());
            DB tx = ((MapdBTx)txh).getTx();
            BTreeMap<DBEntry, DBEntry> map = tx.getTreeMap(name);



            private final Iterator<Map.Entry<DBEntry,DBEntry>> entries = map.subMap(keyStart.as(ENTRY_FACTORY), keyEnd.as(ENTRY_FACTORY)).entrySet().iterator();

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    Map.Entry<DBEntry,DBEntry> ent = entries.next();
                    return new KeyValueEntry(getBuffer(ent.getKey()),getBuffer(ent.getValue()));
                }

                @Override
                public void close() {
//                    tx.commit();
//                    tx.close();
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
//        boolean noTX = (txh == null);
//        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        DB tx = ((MapdBTx) txh).getTx();
        BTreeMap<DBEntry,DBEntry> map = tx.getTreeMap(name);
        map.put(key.as(ENTRY_FACTORY), value.as(ENTRY_FACTORY));
//        if (noTX) {
//            tx.commit();
//            tx.close();}
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) {
//        boolean noTX = (txh == null);
//        DB tx = (noTX ? txMaker.makeTx() : ((MapdBTx) txh).getTx());
        DB tx = ((MapdBTx) txh).getTx();
        tx.getTreeMap(name).remove(key.as(ENTRY_FACTORY));
//        if (noTX) {
//            tx.commit();
//            tx.close();
//        }

    }

    public void clear() {

        DB tx = this.manager.getTxMaker().makeTx();
        tx.getTreeMap(name).clear();
        tx.commit();
        tx.close();
    }

    private static StaticBuffer getBuffer(DBEntry entry) {
        return new StaticArrayBuffer(entry.getData(),entry.getOffset(),entry.getOffset()+entry.getSize());
    }
}

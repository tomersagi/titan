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


import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import javassist.bytecode.stackmap.BasicBlock;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TxMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MapDB based storage manager. Each store-manager is a file based MapDB. Each store is a mapDB named map.
 */
@PreInitializeConfigOptions
public class MapDBStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    public static final ConfigNamespace MapDB_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "mapdb", "MapDB configuration options");
    private static final Logger log = LoggerFactory.getLogger(MapDBStoreManager.class);
    protected final StoreFeatures features;
    private TxMaker txMaker;
    private Map<String,MapDBKeyValueStore> stores;

    public MapDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        txMaker = DBMaker.newFileDB(new File(this.directory, "main.mapdb")).makeTxMaker();
        stores = new HashMap<String, MapDBKeyValueStore>();
        features = new StandardStoreFeatures.Builder()
                .orderedScan(true)
                .transactional(transactional)
                .keyConsistent(GraphDatabaseConfiguration.buildConfiguration())
                .locking(true)
                .keyOrdered(true)
                .build();
    }


    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapdBTx beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        return new MapdBTx(txCfg, getTxMaker());
    }

    @Override
    public MapDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name))
            return stores.get(name);
        stores.put(name,new MapDBKeyValueStore(name,this));
        return stores.get(name);
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, KVMutation> muts : mutations.entrySet()) {
            MapDBKeyValueStore store = openDatabase(muts.getKey());
            KVMutation mut = muts.getValue();
            if (!mut.hasAdditions() && !mut.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", muts.getKey());
            } else {
                log.debug("Mutating {}", muts.getKey());
            }

            if (mut.hasAdditions()) {
                for (KeyValueEntry entry : mut.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", muts.getKey(), entry);
                }
            }
            if (mut.hasDeletions()) {
                for (StaticBuffer del : mut.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", muts.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(MapDBKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unknown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws BackendException {
        for (MapDBKeyValueStore db : stores.values())
            db.close();

        stores.clear();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (MapDBKeyValueStore db : stores.values())
            db.clear();
        stores.clear();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }


    public TxMaker getTxMaker() {
        return txMaker;
    }
}

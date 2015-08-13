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
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TxMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MapDB based storage manager. Each store is a file
 */
@PreInitializeConfigOptions
public class MapDBStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    public static final ConfigNamespace MapDB_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "mapdb", "MapDB configuration options");
    public static final Serializer<DBEntry> DBSER=new DBEntrySerializer();
    public static final Serializer<Object> DBKSER=new DBEntryKeySerializer();
    private static final Logger log = LoggerFactory.getLogger(MapDBStoreManager.class);
    protected final StoreFeatures features;
    private final Map<String, MapDBKeyValueStore> stores;
    private final TxMaker txMaker;

    public MapDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<String, MapDBKeyValueStore>();
        txMaker = DBMaker.fileDB(new File(directory, "titanBackEnd.mapdb")).deleteFilesAfterClose()
                .serializerClassLoader(MapDBStoreManager.class.getClassLoader()).makeTxMaker();
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
        if (!this.stores.values().iterator().hasNext())
            throw new IllegalStateException("Cannot open transaction, no store exists in MapDB");
        return new MapdBTx(txCfg, this.stores.values().iterator().next());
    }

    @Override
    public MapDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            log.debug("Store " +  name + " accessed");
            return stores.get(name);
        }


        MapDBKeyValueStore store = new MapDBKeyValueStore(name, this);
        log.debug("Store " +  name + " created");
        stores.put(name, store);
        return store;
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
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
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
        txMaker.close();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (MapDBKeyValueStore db : stores.values())  {
            db.clear();
            db.close();
        }
        //TODO delete db files

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

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
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import org.mapdb.DB;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapdBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(MapdBTx.class);
    private DB tx = null;
    private TxMaker maker = null;

    public MapdBTx(BaseTransactionConfig config, TxMaker maker) {
        super(config);
        this.maker = maker;
        tx = maker.makeTx();
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        tx.rollback();
        if (tx == null) return;
        if (log.isTraceEnabled())
            log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));
            tx = null;

    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        log.debug("Committed tx: {}",this.toString());
        maker.execute(new TxBlock() {
            @Override
            public void tx(DB txB) throws TxRollbackException {
                tx.commit();
                txB.close();
            }
        });

        if (tx == null) return;
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

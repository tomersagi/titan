package com.thinkaurelius.titan.diskstorage.mapdb;

import org.mapdb.DataIO;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Tomer Sagi on 09-Aug-15.
 * Used as value serializer for MapDB TreeMaps
 */

class DBEntrySerializer extends Serializer<DBEntry> implements Serializable {

    @Override
    public void serialize(DataOutput out, DBEntry value) throws IOException {
        DataIO.packInt(out,value.getSize());
        out.write(value.getData(),value.getOffset(),value.getSize());
    }

    @Override
    public DBEntry deserialize(DataInput in, int available) throws IOException {
        int size = DataIO.unpackInt(in);
        byte[] ret = new byte[size];
        in.readFully(ret);
        return new DBEntry(ret);
    }

    @Override
    public int fixedSize() {
        return 0;
    }
}
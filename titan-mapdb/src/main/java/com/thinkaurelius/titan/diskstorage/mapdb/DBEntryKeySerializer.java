package com.thinkaurelius.titan.diskstorage.mapdb;

import org.mapdb.DBException;
import org.mapdb.DataIO;
import org.mapdb.SerializerBase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.NotSerializableException;

/**
 * Created by Tomer Sagi on 09-Aug-15.
 *
 */

public class DBEntryKeySerializer extends SerializerBase {
    public static int HEADER_DBENTRY=200;
    protected void serializeUnknownObject(DataOutput out, Object obj, FastArrayList<Object> objectStack) throws IOException {
        if (!DBEntry.class.isInstance(obj))
            throw new NotSerializableException("Could not serialize unknown object: "+obj.getClass().getName());

        DBEntry entry = (DBEntry)obj;
        out.write(HEADER_DBENTRY);
        DataIO.packInt(out, entry.getSize());
        out.write(entry.getData(),entry.getOffset(),entry.getSize());
     }

    protected DBEntry deserializeUnknownHeader(DataInput in, int head, FastArrayList<Object> objectStack) throws IOException {
        if (head != HEADER_DBENTRY)
            throw new DBException.DataCorruption("Unknown serialization header: " + head);
        int size = DataIO.unpackInt(in);
        byte[] ret = new byte[size];
        in.readFully(ret);
        DBEntry dbe =new DBEntry(ret);
        objectStack.add(dbe);
        return dbe;
    }


}

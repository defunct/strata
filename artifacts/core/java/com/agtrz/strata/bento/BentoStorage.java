/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata.bento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.agtrz.bento.Bento;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Strata.Storage;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.WeakMapValue;

public class BentoStorage
extends BentoStorageBase
implements Strata.Storage, Serializable
{
    private static final long serialVersionUID = 20070401L;

    private final Schema schema;

    public BentoStorage(Schema schema)
    {
        this.schema = schema;
    }

    public Strata.InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        int blockSize = SizeOf.SHORT + SizeOf.INTEGER + (Bento.ADDRESS_SIZE + schema.getRecordSize()) * (structure.getSchema().getSize() + 1);
        Bento.Address address = mutator.allocate(blockSize);
        Strata.InnerTier inner = new Strata.InnerTier(structure, address, typeOfChildren);
        Object box = address.toKey();
        mapOfTiers.put(box, new WeakMapValue(box, inner, mapOfTiers, queue));
        return inner;
    }

    public Strata.LeafTier newLeafTier(Strata.Structure structure, Object txn)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        int blockSize = SizeOf.INTEGER + (Bento.ADDRESS_SIZE * 2) + (schema.getRecordSize() * structure.getSchema().getSize());
        Bento.Address address = mutator.allocate(blockSize);
        Strata.LeafTier leaf = new Strata.LeafTier(structure, address);
        leaf.setNextLeafKey(Bento.NULL_ADDRESS);
        Object box = address.toKey();
        mapOfTiers.put(box, new WeakMapValue(box, leaf, mapOfTiers, queue));
        return leaf;
    }

    public Strata.InnerTier getInnerTier(Strata.Structure structure, Object txn, Object key)
    {
        collect();

        Bento.Address address = (Bento.Address) key;

        if (address.getPosition() == 0L)
        {
            return null;
        }

        Object box = address.toKey();
        synchronized (this)
        {
            Strata.InnerTier inner = (Strata.InnerTier) getCached(box);

            if (inner == null)
            {
                Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

                ByteBuffer in = mutator.load(address).toByteBuffer();
                short typeOfChildren = in.getShort();

                inner = new Strata.InnerTier(structure, key, typeOfChildren);

                int size = in.getInt();
                if (size != 0)
                {
                    Bento.Address keyOfTier = new Bento.Address(in.getLong(), in.getInt());
                    inner.add(txn, keyOfTier, null);
                    for (int j = 0; j < schema.getRecordSize(); j++)
                    {
                        in.get();
                    }
                }
                for (int i = 1; i < size; i++)
                {
                    Bento.Address keyOfTier = new Bento.Address(in.getLong(), in.getInt());
                    Object object = schema.getReader().read(in);
                    inner.add(txn, keyOfTier, object);
                }

                mapOfTiers.put(box, new WeakMapValue(box, inner, mapOfTiers, queue));
            }

            return inner;
        }
    }

    public Strata.LeafTier getLeafTier(Strata.Structure structure, Object txn, Object key)
    {
        collect();

        Bento.Address address = (Bento.Address) key;

        if (address.getPosition() == 0L)
        {
            return null;
        }

        Object box = address.toKey();
        synchronized (this)
        {
            Strata.LeafTier leaf = (Strata.LeafTier) getCached(box);

            if (leaf == null)
            {
                Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
                leaf = new Strata.LeafTier(structure, address);
                Bento.Block block = mutator.load(address);
                ByteBuffer in = block.toByteBuffer();
                int size = in.getInt();
                leaf.setNextLeafKey(new Bento.Address(in.getLong(), in.getInt()));
                for (int i = 0; i < size; i++)
                {
                    leaf.add(txn, schema.getReader().read(in));
                }

                mapOfTiers.put(box, new WeakMapValue(box, leaf, mapOfTiers, queue));
            }

            return leaf;
        }
    }

    public void write(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

        Bento.Block block = mutator.load((Bento.Address) inner.getStorageData());
        ByteBuffer out = block.toByteBuffer();

        out.putShort(inner.getChildType());
        out.putInt(inner.getSize() + 1);

        for (int i = 0; i < inner.getSize() + 1; i++)
        {
            Strata.Branch branch = inner.get(i);

            if (!(branch.getRightKey() instanceof Bento.Address))
            {
                System.out.println(branch.getRightKey());
            }

            Bento.Address addressOfChild = (Bento.Address) branch.getRightKey();
            out.putLong(addressOfChild.getPosition());
            out.putInt(addressOfChild.getBlockSize());

            if (branch.isMinimal())
            {
                for (int j = 0; j < schema.getRecordSize(); j++)
                {
                    out.put((byte) 0);
                }
            }
            else
            {
                schema.getWriter().write(out, branch.getPivot());
            }
        }

        // for (int i = inner.getSize() + 1; i < structure.getSize() + 1; i++)
        // {
        // out.putLong(0L);
        // out.putInt(0);
        //
        // for (int j = 0; j < recordSize; j++)
        // {
        // out.put((byte) 0);
        // }
        // }

        block.write();
    }

    public void write(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

        Bento.Block block = mutator.load((Bento.Address) leaf.getStorageData());
        ByteBuffer out = block.toByteBuffer();

        out.putInt(leaf.getSize());

        Bento.Address addressOfNext = (Bento.Address) leaf.getNextLeafKey();
        out.putLong(addressOfNext.getPosition());
        out.putInt(addressOfNext.getBlockSize());

        for (int i = 0; i < leaf.getSize(); i++)
        {
            schema.getWriter().write(out, structure.getObjectKey(leaf.get(i)));
        }

        // for (int i = leaf.getSize(); i < structure.getSize(); i++)
        // {
        // for (int j = 0; j < recordSize; j++)
        // {
        // out.put((byte) 0);
        // }
        // }

        block.write();
    }

    public void free(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        Bento.Address address = (Bento.Address) inner.getStorageData();
        mutator.free(mutator.load(address));
    }

    public void free(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        Bento.Address address = (Bento.Address) leaf.getStorageData();
        mutator.free(mutator.load(address));
    }

    public void commit(Object txn)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        mutator.getJournal().commit();
    }

    public Object getKey(Strata.Tier leaf)
    {
        return leaf.getStorageData();
    }

    public Object getNullKey()
    {
        return Bento.NULL_ADDRESS;
    }

    public boolean isKeyNull(Object object)
    {
        return ((Bento.Address) object).getPosition() == 0L;
    }

    public Strata.Storage.Schema getSchema()
    {
        return new Schema(schema);
    }

    private synchronized void collect()
    {
        WeakMapValue reference = null;
        while ((reference = (WeakMapValue) queue.poll()) != null)
        {
            reference.dequeue();
        }
    }

    private Object getCached(Object key)
    {
        WeakMapValue reference = (WeakMapValue) mapOfTiers.get(key);
        if (reference != null)
        {
            return reference.get();
        }
        return null;
    }

    public final static class Schema
    implements Strata.Storage.Schema, Serializable
    {
        private static final long serialVersionUID = 20071018L;

        private Reader reader;

        private Writer writer;

        private int recordSize;

        public Schema()
        {
            this.reader = new BentoAddressReader();
            this.writer = new BentoAddressWriter();
            this.recordSize = Bento.ADDRESS_SIZE;
        }

        public Schema(Schema schema)
        {
            this.reader = schema.reader;
            this.writer = schema.writer;
            this.recordSize = schema.recordSize;
        }

        public void setReader(Reader reader)
        {
            this.reader = reader;
        }

        public Reader getReader()
        {
            return reader;
        }

        public void setWriter(Writer writer)
        {
            this.writer = writer;
        }

        public Writer getWriter()
        {
            return writer;
        }

        public int getRecordSize()
        {
            return recordSize;
        }

        public void setSize(int recordSize)
        {
            this.recordSize = recordSize;
        }

        public Storage newStorage()
        {
            return new BentoStorage(this);
        }
    }

    public static MutatorServer txn(Bento.Mutator mutator)
    {
        return new BasicMutatorServer(mutator);
    }

    public interface MutatorServer
    {
        Bento.Mutator getMutator();
    }

    public final static class BasicMutatorServer
    implements MutatorServer
    {
        private Bento.Mutator mutator;

        public BasicMutatorServer(Bento.Mutator mutator)
        {
            this.mutator = mutator;
        }

        public Bento.Mutator getMutator()
        {
            return mutator;
        }
    }

    public interface Reader
    {
        public Object read(ByteBuffer bytes);
    }

    public interface Writer
    {
        public void write(ByteBuffer bytes, Object object);
    }

    public final static class BentoAddressWriter
    implements Writer
    {
        public void write(ByteBuffer out, Object object)
        {
            Bento.Address address = (Bento.Address) object;
            out.putLong(address.getPosition());
            out.putInt(address.getBlockSize());
        }
    }

    public final static class BentoAddressReader
    implements Reader
    {
        public Object read(ByteBuffer in)
        {
            return new Bento.Address(in.getLong(), in.getInt());
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */

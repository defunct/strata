/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata.bento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.agtrz.bento.Bento;
import com.agtrz.strata.Strata;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.ByteWriter;
import com.agtrz.swag.io.SizeOf;
import com.agtrz.swag.util.Queueable;
import com.agtrz.swag.util.WeakMapValue;

public class BentoStorage
extends BentoStorageBase
implements Strata.Storage, Serializable
{
    private static final long serialVersionUID = 20070401L;

    private final ByteReader reader;

    private final ByteWriter writer;

    public BentoStorage()
    {
        this.reader = new Bento.AddressReader();
        this.writer = new Bento.AddressWriter();
    }

    public BentoStorage(ByteReader reader, ByteWriter writer)
    {
        this.reader = reader;
        this.writer = writer;
    }

    public Strata.InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
    {
        int childSize = writer.getSize(null);
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        int blockSize = SizeOf.SHORT + SizeOf.INTEGER + (SizeOf.INTEGER + Bento.ADDRESS_SIZE + childSize) * (structure.getSize() + 1);
        Bento.Address address = mutator.allocate(blockSize).getAddress();
        Strata.InnerTier inner = new Strata.InnerTier(structure, address, typeOfChildren);
        Object box = address.toKey();
        mapOfTiers.put(box, new WeakMapValue(box, inner, mapOfTiers, queue));
        return inner;
    }

    public Strata.LeafTier newLeafTier(Strata.Structure structure, Object txn)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        int objectSize = writer.getSize(null);
        int blockSize = SizeOf.INTEGER + (Bento.ADDRESS_SIZE * 2) + (objectSize * structure.getSize());
        Bento.Address address = mutator.allocate(blockSize).getAddress();
        Strata.LeafTier leaf = new Strata.LeafTier(structure, address);
        leaf.setPreviousLeafKey(Bento.NULL_ADDRESS);
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
        Strata.InnerTier inner = (Strata.InnerTier) getCached(box);

        if (inner == null)
        {
            Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

            ByteBuffer in = mutator.load(address).toByteBuffer();
            short typeOfChildren = in.getShort();

            inner = new Strata.InnerTier(structure, key, typeOfChildren);

            int size = in.getInt();
            for (int i = 0; i < size; i++)
            {
                int sizeOfChild = in.getInt();
                Bento.Address keyOfTier = new Bento.Address(in.getLong(), in.getInt());
                Object object = reader.read(in);
                inner.add(txn, keyOfTier, object, sizeOfChild);
            }

            mapOfTiers.put(box, new WeakMapValue(box, inner, mapOfTiers, queue));
        }

        return inner;
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
        Strata.LeafTier leaf = (Strata.LeafTier) getCached(box);

        if (leaf == null)
        {
            Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
            leaf = new Strata.LeafTier(structure, address);
            Bento.Block block = mutator.load(address);
            ByteBuffer in = block.toByteBuffer();
            leaf.setPreviousLeafKey(new Bento.Address(in.getLong(), in.getInt()));
            leaf.setNextLeafKey(new Bento.Address(in.getLong(), in.getInt()));
            for (int i = 0; i < structure.getSize(); i++)
            {
                Object object = reader.read(in);
                if (object == null)
                {
                    break;
                }
                leaf.add(object);
            }

            mapOfTiers.put(box, new WeakMapValue(box, leaf, mapOfTiers, queue));
        }

        return leaf;
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
            out.putInt(branch.getSize());

            Bento.Address addressOfChild = (Bento.Address) branch.getLeftKey();
            out.putLong(addressOfChild.getPosition());
            out.putInt(addressOfChild.getBlockSize());

            if (branch.isTerminal())
            {
                writer.write(out, null);
            }
            else
            {
                writer.write(out, branch.getObject());
            }
        }

        for (int i = inner.getSize() + 1; i < structure.getSize() + 1; i++)
        {
            out.putLong(0L);
            out.putInt(0);

            writer.write(out, null);
        }

        block.write();
    }

    public void write(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

        Bento.Block block = mutator.load((Bento.Address) leaf.getStorageData());
        ByteBuffer out = block.toByteBuffer();

        Bento.Address addressOfPrevious = (Bento.Address) leaf.getPreviousLeafKey();
        out.putLong(addressOfPrevious.getPosition());
        out.putInt(addressOfPrevious.getBlockSize());

        Bento.Address addressOfNext = (Bento.Address) leaf.getNextLeafKey();
        out.putLong(addressOfNext.getPosition());
        out.putInt(addressOfNext.getBlockSize());

        for (int i = 0; i < leaf.getSize(); i++)
        {
            writer.write(out, leaf.get(i));
        }

        for (int i = leaf.getSize(); i < structure.getSize(); i++)
        {
            writer.write(out, null);
        }

        block.write();

    }

    public void revert(Strata.Structure structure, Object txn, Strata.LeafTier leaf)
    {
        Bento.Address address = (Bento.Address) leaf.getKey();
        mapOfTiers.remove(address.toKey());
    }

    public void revert(Strata.Structure structure, Object txn, Strata.InnerTier inner)
    {
        Bento.Address address = (Bento.Address) inner.getKey();
        mapOfTiers.remove(address.toKey());
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

    private void collect()
    {
        WeakMapValue reference = null;
        while ((reference = (WeakMapValue) queue.poll()) != null)
        {
            ((Queueable) reference).dequeue();
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

    public final static class Creator
    {
        private ByteReader reader = new Bento.AddressReader();

        private ByteWriter writer = new Bento.AddressWriter();

        public void setReader(ByteReader reader)
        {
            this.reader = reader;
        }

        public void setWriter(ByteWriter writer)
        {
            this.writer = writer;
        }

        public BentoStorage create()
        {
            return new BentoStorage(reader, writer);
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
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
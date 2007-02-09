/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata.bento;

import java.io.Serializable;
import java.nio.ByteBuffer;

import com.agtrz.bento.Bento;
import com.agtrz.strata.Branch;
import com.agtrz.strata.InnerTier;
import com.agtrz.strata.LeafTier;
import com.agtrz.strata.Storage;
import com.agtrz.strata.Strata;
import com.agtrz.strata.Tier;
import com.agtrz.strata.TierLoader;
import com.agtrz.strata.Strata.Structure;
import com.agtrz.swag.io.ByteReader;
import com.agtrz.swag.io.ByteWriter;
import com.agtrz.swag.util.Converter;
import com.agtrz.swag.util.NullConverter;
import com.agtrz.swag.util.WeakMapValueReference;

public class BentoStorage
extends BentoStorageBase
implements Storage, Serializable
{
    private static final long serialVersionUID = 20070208L;

    private final ByteReader reader;

    private final ByteWriter writer;

    public BentoStorage()
    {
        this.reader = new Bento.AddressReader();
        this.writer = new Bento.AddressWriter();
    }

    public BentoStorage(Converter mutatorConverter, Converter addressConverter, ByteReader reader, ByteWriter writer)
    {
        this.reader = reader;
        this.writer = writer;
    }

    public TierLoader getInnerTierLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object txn, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                if (address.getPosition() == 0L)
                {
                    return null;
                }

                Object box = address.toKey();
                InnerTier inner = (InnerTier) getCached(box);

                if (inner == null)
                {
                    Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
                    inner = new BentoInnerTier(structure, mutator, address, reader);
                    mapOfTiers.put(box, new WeakMapValueReference(box, inner, queue));
                }

                return inner;
            }
        };
    }

    public TierLoader getLeafTierLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object txn, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                if (address.getPosition() == 0L)
                {
                    return null;
                }

                Object box = address.toKey();
                LeafTier leaf = (LeafTier) getCached(box);

                if (leaf == null)
                {
                    Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
                    leaf = new BentoLeafTier(structure, mutator, address, reader);
                    mapOfTiers.put(box, new WeakMapValueReference(box, leaf, queue));
                }

                return leaf;
            }
        };

    }

    public InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        InnerTier inner = new BentoInnerTier(structure, mutator, typeOfChildren, writer.getSize(null));
        Object box = ((Bento.Address) inner.getKey()).toKey();
        mapOfTiers.put(box, new WeakMapValueReference(box, inner, queue));
        return inner;
    }

    public LeafTier newLeafTier(Strata.Structure structure, Object txn)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        LeafTier leaf = new BentoLeafTier(structure, mutator, writer.getSize(null));
        Object box = ((Bento.Address) leaf.getKey()).toKey();
        mapOfTiers.put(box, new WeakMapValueReference(box, leaf, queue));
        return leaf;
    }

    public void write(Strata.Structure structure, Object txn, InnerTier inner)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

        Bento.Block block = mutator.load((Bento.Address) inner.getKey());
        ByteBuffer out = block.toByteBuffer();

        out.putShort(inner.getChildType());
        out.putInt(inner.getSize() + 1);

        for (int i = 0; i < inner.getSize() + 1; i++)
        {
            Branch branch = inner.get(i);
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

    public void write(Strata.Structure structure, Object txn, LeafTier leaf)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();

        Bento.Block block = mutator.load((Bento.Address) leaf.getKey());
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

    public void free(Structure structure, Object txn, InnerTier inner)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        Bento.Address address = (Bento.Address) inner.getKey();
        mutator.free(mutator.load(address));
    }

    public void free(Structure structure, Object txn, LeafTier leaf)
    {
        Bento.Mutator mutator = ((MutatorServer) txn).getMutator();
        Bento.Address address = (Bento.Address) leaf.getKey();
        mutator.free(mutator.load(address));
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
        WeakMapValueReference reference = null;
        while ((reference = (WeakMapValueReference) queue.poll()) != null)
        {
            mapOfTiers.remove(reference.getKey());
        }
    }

    private Object getCached(Object key)
    {
        WeakMapValueReference reference = (WeakMapValueReference) mapOfTiers.get(key);
        if (reference != null)
        {
            return reference.get();
        }
        return null;
    }

    public final static class Creator
    {
        private Converter mutatorConverter = NullConverter.INSTANCE;

        private Converter addressConverter = NullConverter.INSTANCE;

        private ByteReader reader = new Bento.AddressReader();

        private ByteWriter writer = new Bento.AddressWriter();

        public void setMutatorConverter(Converter mutatorConverter)
        {
            this.mutatorConverter = mutatorConverter;
        }

        public void setAddressConverter(Converter addressConverter)
        {
            this.addressConverter = addressConverter;
        }

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
            return new BentoStorage(mutatorConverter, addressConverter, reader, writer);
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
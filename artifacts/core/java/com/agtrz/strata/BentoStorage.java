/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;

import com.agtrz.bento.Bento;
import com.agtrz.swag.io.ObjectWriteBuffer;
import com.agtrz.swag.util.Converter;
import com.agtrz.swag.util.NullConverter;
import com.agtrz.swag.util.WeakMapValueReference;
import java.nio.ByteBuffer;

public class BentoStorage
implements Storage
{
    public final static ObjectLoader NULL_OBJECT_LOADER = new ObjectLoader()
    {
        public Object load(Object storage, Object key)
        {
            return key;
        }
    };

    private final ReferenceQueue queue = new ReferenceQueue();

    private final Map mapOfTiers = new HashMap();

    private final Converter mutatorConverter;

    private final Converter addressConverter;

    private final ObjectLoader blockLoader;

    public BentoStorage()
    {
        this.mutatorConverter = NullConverter.INSTANCE;
        this.addressConverter = NullConverter.INSTANCE;
        this.blockLoader = NULL_OBJECT_LOADER;
    }

    public BentoStorage(Converter mutatorConverter, Converter addressConverter, ObjectLoader blockLoader)
    {
        this.mutatorConverter = mutatorConverter;
        this.addressConverter = addressConverter;
        this.blockLoader = blockLoader;
    }

    public TierLoader getInnerTierLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object storage, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                Object box = address.toKey();
                InnerTier tierStorage = (InnerTier) getCached(box);

                if (tierStorage == null)
                {
                    Bento.Mutator mutator = (Bento.Mutator) mutatorConverter.convert(storage);
                    tierStorage = new BentoInnerTier(structure, mutator, address, blockLoader);
                    mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
                }

                return tierStorage;
            }
        };
    }

    public TierLoader getLeafTierLoader()
    {
        return new TierLoader()
        {
            public Tier load(Strata.Structure structure, Object storage, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                Object box = address.toKey();
                LeafTier tierStorage = (LeafTier) getCached(box);

                if (tierStorage == null)
                {
                    Bento.Mutator mutator = (Bento.Mutator) mutatorConverter.convert(storage);
                    tierStorage = new BentoLeafTier(structure, mutator, address, blockLoader);
                    mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
                }

                return tierStorage;
            }
        };

    }

    public InnerTier newInnerTier(Strata.Structure structure, Object txn, short typeOfChildren)
    {
        Bento.Mutator mutator = (Bento.Mutator) mutatorConverter.convert(txn);
        InnerTier tierStorage = new BentoInnerTier(structure, mutator, typeOfChildren);
        Object box = ((Bento.Address) tierStorage.getKey()).toKey();
        mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
        return tierStorage;
    }

    public LeafTier newLeafTier(Strata.Structure structure, Object txn)
    {
        Bento.Mutator mutator = (Bento.Mutator) mutatorConverter.convert(txn);
        LeafTier tierStorage = new BentoLeafTier(structure, mutator);
        Object box = ((Bento.Address) tierStorage.getKey()).toKey();
        mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
        return tierStorage;
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

    public Object getNullKey()
    {
        return Bento.NULL_ADDRESS;
    }

    public boolean isKeyNull(Object object)
    {
        return ((Bento.Address) object).getPosition() == 0L;
    }

    void write(Strata.Structure structure, InnerTier inner, ByteBuffer bytes)
    {
        ObjectWriteBuffer out = new ObjectWriteBuffer(bytes);
        out.write(inner.getChildType());
        for (int i = 0; i < inner.getSize() + 1; i++)
        {
            Branch branch = inner.get(i);
            Bento.Address addressOfChild = (Bento.Address) branch.getKeyOfLeft();
            addressOfChild.write(out);
            if (branch.isTerminal())
            {
                Bento.NULL_ADDRESS.write(out);
            }
            else
            {
                Bento.Address addressOfObject = (Bento.Address) addressConverter.convert(branch.getObject());
                addressOfObject.write(out);
            }
        }
    }

    void write(Strata.Structure structure, LeafTier leaf, ByteBuffer bytes)
    {
        ObjectWriteBuffer out = new ObjectWriteBuffer(bytes);
        Bento.Address addressOfPrevious = (Bento.Address) leaf.getPreviousLeafKey();
        addressOfPrevious.write(out);
        Bento.Address addressOfNext = (Bento.Address) leaf.getNextLeafKey();
        addressOfNext.write(out);
        for (int i = 0; i < leaf.getSize(); i++)
        {
            Bento.Address address = (Bento.Address) addressConverter.convert(leaf.get(i));
            address.write(out);
        }
        for (int i = leaf.getSize(); i < structure.getSize(); i++)
        {
            Bento.NULL_ADDRESS.write(out);
        }
    }

    public final static class Creator
    {
        private Converter mutatorConverter = NullConverter.INSTANCE;

        private ObjectLoader blockLoader;

        private Converter addressConverter = NullConverter.INSTANCE;

        public void setMutatorConverter(Converter mutatorConverter)
        {
            this.mutatorConverter = mutatorConverter;
        }

        public void setBlockLoader(ObjectLoader blockLoader)
        {
            this.blockLoader = blockLoader;
        }

        public void setAddressConverter(Converter addressConverter)
        {
            this.addressConverter = addressConverter;
        }

        public BentoStorage create()
        {
            return new BentoStorage(mutatorConverter, addressConverter, blockLoader);
        }
    }

    public interface ObjectLoader
    {
        public Object load(Object txn, Object key);
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
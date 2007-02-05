/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;

import com.agtrz.bento.Bento;
import com.agtrz.swag.util.Converter;
import com.agtrz.swag.util.WeakMapValueReference;

public class BentoPager
implements Pager
{
    private final ReferenceQueue queue = new ReferenceQueue();

    private final Map mapOfTiers = new HashMap();

    private final Converter converter;

    public BentoPager(Converter objectConverter)
    {
        this.converter = objectConverter;
    }

    public PageLoader getInnerPageLoader()
    {
        return new PageLoader()
        {
            public Tier load(Storage storage, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                Object box = address.toKey();
                InnerTier tierStorage = (InnerTier) getCached(box);

                if (tierStorage == null)
                {
                    tierStorage = new BentoInnerPage(storage, address, converter);
                    mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
                }

                return tierStorage;
            }
        };
    }

    public PageLoader getLeafPageLoader()
    {
        return new PageLoader()
        {
            public Tier load(Storage storage, Object key)
            {
                collect();

                Bento.Address address = (Bento.Address) key;

                Object box = address.toKey();
                LeafTier tierStorage = (LeafTier) getCached(box);

                if (tierStorage == null)
                {
                    tierStorage = new BentoLeafPage(storage, address, converter);
                    mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
                }

                return tierStorage;
            }
        };

    }

    public InnerTier newInnerPage(Storage storage, short typeOfChildren)
    {
        InnerTier tierStorage = new BentoInnerPage(storage, typeOfChildren);
        Object box = ((Bento.Address) tierStorage.getKey()).toKey();
        mapOfTiers.put(box, new WeakMapValueReference(box, tierStorage, queue));
        return tierStorage;
    }

    public LeafTier newLeafPage(Storage storage)
    {
        LeafTier tierStorage = new BentoLeafPage(storage);
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
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
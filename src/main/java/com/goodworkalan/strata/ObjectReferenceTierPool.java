package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

// TODO Document.
final class ObjectReferenceTierPool<T>
implements Pool<T, Ilk.Pair>
{
    // TODO Document.
    private final Ilk.Key key;
    
    // TODO Document.
    public ObjectReferenceTierPool(Ilk.Key key)
    {
        this.key = key;
    }
    
    // TODO Document.
    public InnerTier<T, Ilk.Pair> getInnerTier(Stash stash, Ilk.Pair address)
    {
        return address.cast(new Ilk<InnerTier<T, Ilk.Pair>>(key) { });
    }
    
    // TODO Document.
    public LeafTier<T, Ilk.Pair> getLeafTier(Stash stash, Ilk.Pair address)
    {
        return address.cast(new Ilk<LeafTier<T, Ilk.Pair>>(key) { });
    }
}
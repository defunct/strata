package com.goodworkalan.strata;

import com.goodworkalan.ilk.Ilk;
import com.goodworkalan.stash.Stash;

// TODO Document.
final class NullAllocator<T>
implements Allocator<T, Ilk.Pair>
{
    // TODO Document.
    private final Ilk.Key key;
    
    // TODO Document.
    public NullAllocator(Ilk.Key key)
    {
        this.key = key;
    }
    
    // TODO Document.
    public Ilk.Pair allocate(Stash stash, InnerTier<T, Ilk.Pair> inner, int size)
    {
        return new Ilk<InnerTier<T, Ilk.Pair>>(key) { }.pair(inner);
    }
    
    // TODO Document.
    public Ilk.Pair allocate(Stash stash, LeafTier<T, Ilk.Pair> leaf, int size)
    {
        return new Ilk<LeafTier<T, Ilk.Pair>>(key) { }.pair(leaf);
    }
    
    // TODO Document.
    public void load(Stash stash, Ilk.Pair address, InnerTier<T, Ilk.Pair> inner)
    {
    }
    
    // TODO Document.
    public void load(Stash stash, Ilk.Pair address, LeafTier<T, Ilk.Pair> leaf)
    {
    }

    // TODO Document.
    public void dirty(Stash stash, InnerTier<T, Ilk.Pair> inner)
    {
    }

    // TODO Document.
    public void dirty(Stash stash, LeafTier<T, Ilk.Pair> leaf)
    {
    }

    // TODO Document.
    public void remove(Stash stash, InnerTier<T, Ilk.Pair> inner)
    {
    }

    // TODO Document.
    public void remove(Stash stash, LeafTier<T, Ilk.Pair> leaf)
    {
    }

    // TODO Document.
    public boolean isNull(Ilk.Pair address)
    {
        return address == null;
    }
    
    // TODO Document.
    public Ilk.Pair getNull()
    {
        return null;
    }
}
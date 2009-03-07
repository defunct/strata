package com.goodworkalan.strata;

// TODO Document.
final class ObjectReferenceTierPoolBuilder
implements TierPoolBuilder
{
    // TODO Document.
    public <B, T, F extends Comparable<? super F>, A> TierPool<B, A> newTierPool(Build<B, T, F, A> build)
    {
        return new ObjectReferenceTierPool<B, A>();
    }
}
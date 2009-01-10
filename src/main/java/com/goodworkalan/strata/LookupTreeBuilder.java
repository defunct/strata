package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;


class LookupTreeBuilder
implements TreeBuilder
{
    public <T, F extends Comparable<F>, A> Query<T, F> newTransaction(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage)
    {
        Cooper<T, F, T> cooper = new LookupCooper<T, F>();
        Build<T, T, F, A> build = new Build<T, T, F, A>(schema, storage, cooper);
        return build.newTransaction(stash);
    }
}
package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
class LookupTreeBuilder
implements TreeBuilder
{
    // TODO Document.
    public <T, F extends Comparable<? super F>, A> Construction<T, F, A> create(Stash stash, Schema<T, F> schema, Storage<T, F, A> storage)
    {
        Cooper<T, F, T> cooper = new LookupCooper<T, F>();
        Build<T, T, F, A> build = new Build<T, T, F, A>(schema, storage, cooper);
        return build.create(stash);
    }
    
    // TODO Document.
    public <T, F extends Comparable<? super F>, A> com.goodworkalan.strata.Strata<T,F> open(Stash stash, com.goodworkalan.strata.Schema<T,F> schema, com.goodworkalan.strata.Storage<T,F,A> storage, A rootAddress)
    {
        Cooper<T, F, T> cooper = new LookupCooper<T, F>();
        return new Build<T, T, F, A>(schema, storage, cooper).open(stash, rootAddress);
    }
}
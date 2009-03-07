package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class CoreStrata<B, T, F extends Comparable<? super F>, A>
implements Strata<T, F>
{
    // TODO Document.
    private final A rootAddress;
    
    // TODO Document.
    private final Structure<B, A> structure;

    // TODO Document.
    private final Cooper<T, F, B> cooper;
    
    // TODO Document.
    private final Extractor<T, F> extractor;
    
    // TODO Document.
    private final Schema<T, F> schema;
    
    // TODO Document.
    public CoreStrata(A rootAddress, Schema<T, F> schema, Build<B, T, F, A> build)
    {
        this.rootAddress = rootAddress;
        this.schema = schema;
        this.cooper = build.getCooper();
        this.structure = build;
        this.extractor = schema.getExtractor();
    }
    
    // TODO Document.
    public A getRootAddress()
    {
        return rootAddress;
    }
    
    // TODO Document.
    public Schema<T, F> getSchema()
    {
        return schema;
    }
    
    // TODO Document.
    public Query<T, F> query()
    {
        return query(new Stash());
    }

    // TODO Document.
    public Query<T, F> query(Stash stash)
    {
        return new CoreQuery<B, T, F, A>(stash, this, structure);
    }

    // TODO Document.
    public Cooper<T, F, B> getCooper()
    {
        return cooper;
    }
    
    // TODO Document.
    public Extractor<T, F> getExtractor()
    {
        return extractor;
    }
}
package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;


public final class CoreTree<B, T, F extends Comparable<F>, A>
implements Strata<T, F>
{
    private final A rootAddress;
    
    private final Structure<B, A> structure;

    private final Cooper<T, F, B> cooper;
    
    private final Extractor<T, F> extractor;
    
    private final Schema<T, F> schema;
    
    public CoreTree(A rootAddress, Schema<T, F> schema, Build<B, T, F, A> build)
    {
        this.rootAddress = rootAddress;
        this.schema = schema;
        this.cooper = build.getCooper();
        this.structure = build;
        this.extractor = schema.getExtractor();
    }
    
    public A getRootAddress()
    {
        return rootAddress;
    }
    
    public Schema<T, F> getSchema()
    {
        return schema;
    }
    
    public Query<T, F> query()
    {
        return query(new Stash());
    }

    public Query<T, F> query(Stash stash)
    {
        return new CoreQuery<B, T, F, A>(stash, this, structure);
    }

    public Cooper<T, F, B> getCooper()
    {
        return cooper;
    }
    
    public Extractor<T, F> getExtractor()
    {
        return extractor;
    }
}
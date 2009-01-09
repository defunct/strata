package com.goodworkalan.strata;


public final class CoreTree<B, T, F extends Comparable<F>, A, X>
implements Strata<T, F, X>
{
    private final A rootAddress;
    
    private final Structure<B, A, X> structure;

    private final Cooper<T, F, B, X> cooper;
    
    private final Extractor<T, F, X> extractor;
    
    private final Schema<T, F, X> schema;
    
    public CoreTree(A rootAddress, Schema<T, F, X> schema, Build<B, T, F, A, X> build)
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
    
    public Schema<T, F, X> getSchema()
    {
        return schema;
    }

    public Transaction<T, F, X> query(X txn)
    {
        return new CoreQuery<B, T, F, A, X>(txn, this, structure);
    }

    public Cooper<T, F, B, X> getCooper()
    {
        return cooper;
    }
    
    public Extractor<T, F, X> getExtractor()
    {
        return extractor;
    }
}
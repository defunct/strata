package com.goodworkalan.strata;


public final class CoreTree<B, T, A, X>
implements Strata<T, X>
{
    private final A rootAddress;
    
    private final Structure<B, A, X> structure;

    private final Cooper<T, B, X> cooper;
    
    private final Extractor<T, X> extractor;
    
    private final Schema<T, X> schema;
    
    public CoreTree(A rootAddress, Schema<T, X> schema, Build<B, T, A, X> build)
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
    
    public Schema<T, X> getSchema()
    {
        return schema;
    }

    public Transaction<T, X> query(X txn)
    {
        return new CoreQuery<B, T, A, X>(txn, this, structure);
    }

    public Cooper<T, B, X> getCooper()
    {
        return cooper;
    }
    
    public Extractor<T, X> getExtractor()
    {
        return extractor;
    }
}
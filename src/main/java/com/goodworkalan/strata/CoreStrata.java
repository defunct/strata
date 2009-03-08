package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class CoreStrata<T, A>
implements Strata<T>
{
    // TODO Document.
    private final A rootAddress;
    
    // TODO Document.
    private final Structure<T, A> structure;

    /**
     * Create a new b+tree implementation with the given root address and
     * bouquet of services.
     * 
     * @param rootAddress
     *            The root address of the b+tree root inner tier.
     * @param structure
     *            A bouquet of services.
     */
    public CoreStrata(A rootAddress, Structure<T, A> structure)
    {
        this.rootAddress = rootAddress;
        // FIXME This is null.
        this.structure = structure;
    }
    
    // TODO Document.
    public A getRootAddress()
    {
        return rootAddress;
    }
    
    // TODO Document.
    public Schema<T> getSchema()
    {
        return structure.newSchema();
    }
    
    // TODO Document.
    public Query<T> query()
    {
        return query(new Stash());
    }

    // TODO Document.
    public Query<T> query(Stash stash)
    {
        return new CoreQuery<T, A>(stash, this, structure);
    }
}
package com.goodworkalan.strata;

import com.goodworkalan.favorites.Stash;

public final class Schema<T, F extends Comparable<F>>
{
    private int innerSize;
    
    private int leafSize;
    
    private StorageBuilder<T, F> storageBuilder;
    
    private AllocatorBuilder allocatorBuilder;
    
    private TierWriterBuilder tierWriterBuilder;
    
    private TierPoolBuilder tierPoolBuilder;
    
    private boolean fieldCaching;
    
    private Extractor<T, F> extractor;
    
    public void setInnerSize(int innerSize)
    {
        this.innerSize = innerSize;
    }
    
    public int getInnerSize()
    {
        return innerSize;
    }

    public void setLeafSize(int leafSize)
    {
        this.leafSize = leafSize;
    }
    
    public int getLeafSize()
    {
        return leafSize;
    }
    
    public AllocatorBuilder getAllocatorBuilder()
    {
        return allocatorBuilder;
    }

    public void setAllocatorBuilder(AllocatorBuilder allocatorBuilder)
    {
        this.allocatorBuilder = allocatorBuilder;
    }
    
    public boolean isFieldCaching()
    {
        return fieldCaching;
    }
    
    public void setFieldCaching(boolean fieldCaching)
    {
        this.fieldCaching = fieldCaching;
    }
    
    public StorageBuilder<T, F> getStorageBuilder()
    {
        return storageBuilder;
    }
    
    public void setStorageBuilder(StorageBuilder<T, F> storageBuilder)
    {
        this.storageBuilder = storageBuilder;
    }
    
    public TierWriterBuilder getTierWriterBuilder()
    {
        return tierWriterBuilder;
    }
    
    public void setTierWriterBuilder(TierWriterBuilder tierWriterBuilder)
    {
        this.tierWriterBuilder = tierWriterBuilder;
    }
    
    public TierPoolBuilder getTierPoolBuilder()
    {
        return tierPoolBuilder;
    }
    
    public void setTierPoolBuilder(TierPoolBuilder tierPoolBuilder)
    {
        this.tierPoolBuilder = tierPoolBuilder;
    }

    public Extractor<T, F> getExtractor()
    {
        return extractor;
    }

    public void setExtractor(Extractor<T, F> extractor)
    {
        this.extractor = extractor;
    }
    
    public <A> Query<T, F> newTransaction(Stash stash, Storage<T, F, A> storage)
    {
        TreeBuilder builder = isFieldCaching() ? new BucketTreeBuilder() : new LookupTreeBuilder();
        return builder.newTransaction(stash, this, storage);
    }
    
    public Query<T, F> newTransaction(Stash stash)
    {
        return storageBuilder.newTransaction(stash, this);
    }
}
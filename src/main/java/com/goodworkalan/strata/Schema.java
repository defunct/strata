package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class Schema<T, F extends Comparable<? super F>>
{
    // TODO Document.
    private int innerSize;
    
    // TODO Document.
    private int leafSize;
    
    // TODO Document.
    private StorageBuilder<T, F> storageBuilder;
    
    // TODO Document.
    private AllocatorBuilder allocatorBuilder;
    
    // TODO Document.
    private TierWriterBuilder tierWriterBuilder;
    
    // TODO Document.
    private TierPoolBuilder tierPoolBuilder;
    
    // TODO Document.
    private boolean fieldCaching;
    
    // TODO Document.
    private Extractor<T, F> extractor;
    
    // TODO Document.
    public void setInnerSize(int innerSize)
    {
        this.innerSize = innerSize;
    }
    
    // TODO Document.
    public int getInnerSize()
    {
        return innerSize;
    }

    // TODO Document.
    public void setLeafSize(int leafSize)
    {
        this.leafSize = leafSize;
    }
    
    // TODO Document.
    public int getLeafSize()
    {
        return leafSize;
    }
    
    // TODO Document.
    public AllocatorBuilder getAllocatorBuilder()
    {
        return allocatorBuilder;
    }

    // TODO Document.
    public void setAllocatorBuilder(AllocatorBuilder allocatorBuilder)
    {
        this.allocatorBuilder = allocatorBuilder;
    }
    
    // TODO Document.
    public boolean isFieldCaching()
    {
        return fieldCaching;
    }
    
    // TODO Document.
    public void setFieldCaching(boolean fieldCaching)
    {
        this.fieldCaching = fieldCaching;
    }
    
    // TODO Document.
    public StorageBuilder<T, F> getStorageBuilder_()
    {
        return storageBuilder;
    }
    
    // TODO Document.
    public void setStorageBuilder_(StorageBuilder<T, F> storageBuilder)
    {
        this.storageBuilder = storageBuilder;
    }
    
    // TODO Document.
    public TierWriterBuilder getTierWriterBuilder()
    {
        return tierWriterBuilder;
    }
    
    // TODO Document.
    public void setTierWriterBuilder(TierWriterBuilder tierWriterBuilder)
    {
        this.tierWriterBuilder = tierWriterBuilder;
    }
    
    // TODO Document.
    public TierPoolBuilder getTierPoolBuilder()
    {
        return tierPoolBuilder;
    }
    
    // TODO Document.
    public void setTierPoolBuilder(TierPoolBuilder tierPoolBuilder)
    {
        this.tierPoolBuilder = tierPoolBuilder;
    }

    // TODO Document.
    public Extractor<T, F> getExtractor()
    {
        return extractor;
    }

    // TODO Document.
    public void setExtractor(Extractor<T, F> extractor)
    {
        this.extractor = extractor;
    }
    
    // TODO Document.
    public <A> Construction<T, F, A> create(Stash stash, Storage<T, F, A> storage)
    {
        TreeBuilder builder = isFieldCaching() ? new BucketTreeBuilder() : new LookupTreeBuilder();
        return builder.create(stash, this, storage);
    }
    
    // TODO Document.
    public <A> Strata<T, F> open(Stash stash, Storage<T, F, A> storage, A address) 
    {
        TreeBuilder builder = isFieldCaching() ? new BucketTreeBuilder() : new LookupTreeBuilder();
        return builder.open(stash, this, storage, address);
    }
    
    // TODO Document.
    public Query<T, F> create(Stash stash, StorageBuilder<T, F> storageBuilder)
    {
        return storageBuilder.create(stash, this);
    }

//    public Query<T, F> create(Stash stash)
//    {
//        return storageBuilder.create(stash, this);
//    }
}
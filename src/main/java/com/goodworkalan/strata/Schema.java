package com.goodworkalan.strata;

public final class Schema<T, X>
{
    private int innerSize;
    
    private int leafSize;
    
    private StorageBuilder<T, X> storageBuilder;
    
    private AllocatorBuilder allocatorBuilder;
    
    private TierWriterBuilder tierWriterBuilder;
    
    private TierPoolBuilder tierPoolBuilder;
    
    private boolean fieldCaching;
    
    private Extractor<T, X> extractor;
    
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
    
    public StorageBuilder<T, X> getStorageBuilder()
    {
        return storageBuilder;
    }
    
    public void setStorageBuilder(StorageBuilder<T, X> storageBuilder)
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

    public Extractor<T, X> getExtractor()
    {
        return extractor;
    }

    public void setExtractor(Extractor<T, X> extractor)
    {
        this.extractor = extractor;
    }
    
    public <A> Transaction<T, X> newTransaction(X txn, Storage<T, A, X> storage)
    {
        TreeBuilder builder = isFieldCaching() ? new BucketTreeBuilder() : new LookupTreeBuilder();
        return builder.newTransaction(txn, this, storage);
    }
    
    public Transaction<T, X> newTransaction(X txn)
    {
        return storageBuilder.newTransaction(txn, this);
    }
}
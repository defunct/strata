package com.goodworkalan.strata;

public final class Schema<T, F extends Comparable<F>, X>
{
    private int innerSize;
    
    private int leafSize;
    
    private StorageBuilder<T, F, X> storageBuilder;
    
    private AllocatorBuilder allocatorBuilder;
    
    private TierWriterBuilder tierWriterBuilder;
    
    private TierPoolBuilder tierPoolBuilder;
    
    private boolean fieldCaching;
    
    private Extractor<T, F, X> extractor;
    
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
    
    public StorageBuilder<T, F, X> getStorageBuilder()
    {
        return storageBuilder;
    }
    
    public void setStorageBuilder(StorageBuilder<T, F, X> storageBuilder)
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

    public Extractor<T, F, X> getExtractor()
    {
        return extractor;
    }

    public void setExtractor(Extractor<T, F, X> extractor)
    {
        this.extractor = extractor;
    }
    
    public <A> Transaction<T, F, X> newTransaction(X txn, Storage<T, F, A, X> storage)
    {
        TreeBuilder builder = isFieldCaching() ? new BucketTreeBuilder() : new LookupTreeBuilder();
        return builder.newTransaction(txn, this, storage);
    }
    
    public Transaction<T, F, X> newTransaction(X txn)
    {
        return storageBuilder.newTransaction(txn, this);
    }
}
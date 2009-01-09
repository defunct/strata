package com.goodworkalan.strata;


public final class Build<B, T, F extends Comparable<F>, A, X>
implements Structure<B, A, X>
{
    private final Schema<T, F, X> schema;

    private final Storage<T, F, A, X> storage;
    
    private final Cooper<T, F, B, X> cooper;
    
    private final Allocator<B, A, X> allocator;
    
    private final TierWriter<B, A, X> writer;
    
    private final TierPool<B, A, X> pool;
    
    public Build(Schema<T, F, X> schema, Storage<T, F, A, X> storage, Cooper<T, F, B, X> cooper)
    {
        this.schema = schema;
        this.storage = storage;
        this.cooper = cooper;
        this.allocator = schema.getAllocatorBuilder().newAllocator(this);
        this.writer = schema.getTierWriterBuilder().newTierWriter(this);
        this.pool = schema.getTierPoolBuilder().newTierPool(this);
    }
    
    public int getInnerSize()
    {
        return schema.getInnerSize();
    }
    
    public int getLeafSize()
    {
        return schema.getLeafSize();
    }

    public Schema<T, F, X> getSchema()
    {
        return schema;
    }
    
    public Storage<T, F, A, X> getStorage()
    {
        return storage;
    }
    
    public Cooper<T, F, B, X> getCooper()
    {
        return cooper;
    }
    
    public Allocator<B, A, X> getAllocator()
    {
        return allocator;
    }

    public TierWriter<B, A, X> getWriter()
    {
        return writer;
    }
    
    public TierPool<B, A, X> getPool()
    {
        return pool;
    }
    
    public int compare(X txn, B left, B right)
    {
        Extractor<T, F, X> extractor = schema.getExtractor();
        return getCooper().getFields(txn, extractor, left).compareTo(getCooper().getFields(txn, extractor, right));
    }
    
    public Transaction<T, F, X> newTransaction(X txn)
    {
        writer.begin();
        
        InnerTier<B, A> root = new InnerTier<B, A>();
        root.setChildType(ChildType.LEAF);
        root.setAddress(allocator.allocate(txn, root, schema.getInnerSize()));
        
        LeafTier<B, A> leaf = new LeafTier<B, A>();
        leaf.setAddress(allocator.allocate(txn, leaf, schema.getLeafSize()));
        
        root.add(new Branch<B, A>(null, leaf.getAddress()));

        writer.dirty(txn, root);
        writer.dirty(txn, leaf);
        writer.end(txn);
        
        CoreTree<B, T, F, A, X> tree = new CoreTree<B, T, F, A, X>(root.getAddress(), schema, this);
        
        return new CoreQuery<B, T, F, A, X>(txn, tree, this);
    }
}
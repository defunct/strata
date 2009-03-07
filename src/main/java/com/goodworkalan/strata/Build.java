package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

// TODO Document.
public final class Build<B, T, F extends Comparable<? super F>, A>
implements Structure<B, A>
{
    // TODO Document.
    private final Schema<T, F> schema;

    // TODO Document.
    private final Storage<T, F, A> storage;
    
    // TODO Document.
    private final Cooper<T, F, B> cooper;
    
    // TODO Document.
    private final Allocator<B, A> allocator;
    
    // TODO Document.
    private final TierWriter<B, A> writer;
    
    // TODO Document.
    private final TierPool<B, A> pool;
    
    // TODO Document.
    public Build(Schema<T, F> schema, Storage<T, F, A> storage, Cooper<T, F, B> cooper)
    {
        this.schema = schema;
        this.storage = storage;
        this.cooper = cooper;
        this.allocator = schema.getAllocatorBuilder().newAllocator(this);
        this.writer = schema.getTierWriterBuilder().newTierWriter(this);
        this.pool = schema.getTierPoolBuilder().newTierPool(this);
    }
    
    // TODO Document.
    public int getInnerSize()
    {
        return schema.getInnerSize();
    }
    
    // TODO Document.
    public int getLeafSize()
    {
        return schema.getLeafSize();
    }

    // TODO Document.
    public Schema<T, F> getSchema()
    {
        return schema;
    }
    
    // TODO Document.
    public Storage<T, F, A> getStorage()
    {
        return storage;
    }
    
    // TODO Document.
    public Cooper<T, F, B> getCooper()
    {
        return cooper;
    }
    
    // TODO Document.
    public Allocator<B, A> getAllocator()
    {
        return allocator;
    }

    // TODO Document.
    public TierWriter<B, A> getWriter()
    {
        return writer;
    }
    
    // TODO Document.
    public TierPool<B, A> getPool()
    {
        return pool;
    }
    
    // TODO Document.
    public int compare(Stash stash, B left, B right)
    {
        Extractor<T, F> extractor = schema.getExtractor();
        return getCooper().getFields(stash, extractor, left).compareTo(getCooper().getFields(stash, extractor, right));
    }
    
    // TODO Document.
    public Construction<T, F, A> create(Stash stash)
    {
        writer.begin();
        
        InnerTier<B, A> root = new InnerTier<B, A>();
        root.setChildType(ChildType.LEAF);
        root.setAddress(allocator.allocate(stash, root, schema.getInnerSize()));
        
        LeafTier<B, A> leaf = new LeafTier<B, A>();
        leaf.setAddress(allocator.allocate(stash, leaf, schema.getLeafSize()));
        
        root.add(new Branch<B, A>(null, leaf.getAddress()));

        writer.dirty(stash, root);
        writer.dirty(stash, leaf);
        writer.end(stash);
        
        CoreStrata<B, T, F, A> tree = new CoreStrata<B, T, F, A>(root.getAddress(), schema, this);
        
        return new Construction<T, F, A>(new CoreQuery<B, T, F, A>(stash, tree, this), root.getAddress());
    }
    
    // TODO Document.
    public Strata<T, F> open(Stash stash, A rootAddress)
    {
        return new CoreStrata<B, T, F, A>(rootAddress, schema, this);
    }
}
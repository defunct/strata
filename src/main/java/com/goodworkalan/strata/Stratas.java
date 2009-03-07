/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;

// TODO Document.
public class Stratas
{
    // TODO Document.
    public static AllocatorBuilder newNullAllocatorBuilder()
    {
        return new NullAllocatorBuilder();
    }

    // TODO Document.
    public static AllocatorBuilder newStorageAllocatorBuilder()
    {
        return new StorageAllocatorBuilder();
    }

    // TODO Document.
    public static TierWriterBuilder newEmptyTierWriter()
    {
        return new EmptyTierWriterBuilder();
    }

    // TODO Document.
    public static TierWriterBuilder newPerQueryTierWriter(int max)
    {
        return new PerQueryTierWriterBuilder(max);
    }

    // TODO Document.
    public static TierWriterBuilder newPerStrataTierWriter(int max)
    {
        return new PerStrataTierWriterBuilder(max);
    }
    
    // TODO Document.
    public static TierPoolBuilder newBasicTierPool()
    {
        return new BasicTierPoolBuilder();
    }
    
    // TODO Document.
    public static TierPoolBuilder newObjectReferenceTierPool()
    {
        return new ObjectReferenceTierPoolBuilder();
    }
    
    // TODO Document.
    public static <T, F extends Comparable<? super F>> Schema<T, F> newInMemorySchema()
    {
        Schema<T, F> schema = new Schema<T, F>();
        schema.setAllocatorBuilder(newNullAllocatorBuilder());
        schema.setFieldCaching(false);
//        schema.setStorageBuilder(new InMemoryStorageBuilder<T, F>());
        schema.setTierPoolBuilder(newObjectReferenceTierPool());
        schema.setTierWriterBuilder(newEmptyTierWriter());
        return schema;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
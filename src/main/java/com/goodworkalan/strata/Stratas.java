/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;


public class Stratas
{
    public static AllocatorBuilder newNullAllocatorBuilder()
    {
        return new NullAllocatorBuilder();
    }

    public static AllocatorBuilder newStorageAllocatorBuilder()
    {
        return new StorageAllocatorBuilder();
    }

    public static TierWriterBuilder newEmptyTierWriter()
    {
        return new EmptyTierWriterBuilder();
    }

    public static TierWriterBuilder newPerQueryTierWriter(int max)
    {
        return new PerQueryTierWriterBuilder(max);
    }

    public static TierWriterBuilder newPerStrataTierWriter(int max)
    {
        return new PerStrataTierWriterBuilder(max);
    }
    
    public static TierPoolBuilder newBasicTierPool()
    {
        return new BasicTierPoolBuilder();
    }
    
    public static TierPoolBuilder newObjectReferenceTierPool()
    {
        return new ObjectReferenceTierPoolBuilder();
    }
    
    public static <T, F extends Comparable<F>> Schema<T, F> newInMemorySchema()
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
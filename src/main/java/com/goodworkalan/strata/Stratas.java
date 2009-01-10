/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.strata;


public class Stratas
{
    @SuppressWarnings("unchecked")
    private final static int compare(Object left, Object right)
    {
        return ((Comparable) left).compareTo(right);
    }

    final static int compare(Comparable<?>[] left, Comparable<?>[] right)
    {
        if (left == null)
        {
            if (right == null)
            {
                throw new IllegalStateException();
            }
            return -1;
        }
        else if (right == null)
        {
            return 1;
        }

        int count = Math.min(left.length, right.length);
        for (int i = 0; i < count; i++)
        {
            if (left[i] == null)
            {
                if (right[i] != null)
                {
                    return -1;
                }
            }
            else if (right[i] == null)
            {
                return 1;
            }
            else
            {
                int compare = compare(left[i], right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
        }

        return left.length - right.length;
    }

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
        schema.setStorageBuilder(new InMemoryStorageBuilder<T, F>());
        schema.setTierPoolBuilder(newObjectReferenceTierPool());
        schema.setTierWriterBuilder(newEmptyTierWriter());
        return schema;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
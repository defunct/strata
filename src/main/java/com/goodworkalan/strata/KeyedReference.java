package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Map;

final class KeyedReference<A, T>
extends SoftReference<T> implements Unmappable
{
    private final A key;
    
    private final Map<A, ?> map;
    
    public KeyedReference(A key, T object, Map<A, Reference<T>> map, ReferenceQueue<T> queue)
    {
        super(object, queue);
        this.key = key;
        this.map = map;
    }
    
    public void unmap()
    {
        map.remove(key);
    }
}
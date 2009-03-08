package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Map;

// TODO Document.
final class KeyedReference<T, A>
extends SoftReference<T> implements Unmappable
{
    // TODO Document.
    private final A key;
    
    // TODO Document.
    private final Map<A, ?> map;
    
    // TODO Document.
    public KeyedReference(A key, T object, Map<A, Reference<T>> map, ReferenceQueue<T> queue)
    {
        super(object, queue);
        this.key = key;
        this.map = map;
    }
    
    // TODO Document.
    public void unmap()
    {
        map.remove(key);
    }
}
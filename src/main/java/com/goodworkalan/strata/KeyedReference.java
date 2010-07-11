package com.goodworkalan.strata;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.Map;

/**
 * A soft reference to an object that is stored as a value in a map that has the
 * ability to remove itself from the map when the soft reference is no longer
 * referenced and added to a reference queue. A loop will poll the reference
 * queue, removing keyed references and calling the unmap method to remove them
 * from the map.
 * 
 * @author Alan Gutierrez
 * 
 * @param <A>
 *            The key type.
 * @param <K>
 *            The softly referenced value type.
 */
final class KeyedReference<A, K>
extends SoftReference<K> implements Unmappable {
    /** The key used to map the soft reference. */
    private final A key;
    
    /** The map where the soft reference is stored. */
    private final Map<A, ?> map;

    /**
     * 
     * @param key
     *            The key used to map the soft reference.
     * @param object
     *            The object to reference softly.
     * @param map
     *            The map where the soft reference is stored.
     * @param queue
     *            The reference queue used to track when the object is no longer
     *            hard referenced.
     */
    public KeyedReference(A key, K object, Map<A, Reference<K>> map, ReferenceQueue<K> queue) {
        super(object, queue);
        this.key = key;
        this.map = map;
    }

    /**
     * Remove the keyed reference value from the by removing the value in the
     * map property keyed by the key property.
     */
    public void unmap() {
        map.remove(key);
    }
}
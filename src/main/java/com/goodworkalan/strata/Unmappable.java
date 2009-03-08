package com.goodworkalan.strata;

/**
 * An interface implemented by keyed references to that will remove the soft
 * reference form a map.
 * 
 * @author Alan Gutierrez
 */
interface Unmappable
{
    /**
     * Removes this object from a map.
     */
    public void unmap();
}
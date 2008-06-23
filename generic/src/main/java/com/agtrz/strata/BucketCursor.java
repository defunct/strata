/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class BucketCursor<T, F extends Comparable<? super F>, B, X>
implements Cursor<T>
{
    public Cooper<T, F, B, X> cooper;
    
    private Cursor<B> cursor;
    
    public BucketCursor(Cursor<B> cursor)
    {
    }
    
    public T next()
    {
        return cooper.getObject(cursor.next());
    }
    
    
    public boolean hasNext()
    {
        return cursor.hasNext();
    }
    
    public void remove()
    {
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
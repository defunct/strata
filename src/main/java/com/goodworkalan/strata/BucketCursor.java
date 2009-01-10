package com.goodworkalan.strata;

public final class BucketCursor<T, F extends Comparable<F>, B>
implements Cursor<T>
{
    public Cooper<T, F, B> cooper;

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
    
    public void release()
    {
    }
}
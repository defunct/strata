package com.goodworkalan.strata;

public final class BucketCursor<T, B, X>
implements Cursor<T>
{
    public Cooper<T, B, X> cooper;

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
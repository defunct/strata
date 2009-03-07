package com.goodworkalan.strata;

// TODO Document.
public final class BucketCursor<T, F extends Comparable<? super F>, B>
implements Cursor<T>
{
    // TODO Document.
    public Cooper<T, F, B> cooper;

    // TODO Document.
    private Cursor<B> cursor;

    // TODO Document.
    public BucketCursor(Cursor<B> cursor)
    {
    }

    // TODO Document.
    public T next()
    {
        return cooper.getObject(cursor.next());
    }

    // TODO Document.
    public boolean hasNext()
    {
        return cursor.hasNext();
    }

    // TODO Document.
    public void remove()
    {
    }
    
    // TODO Document.
    public void release()
    {
    }
}
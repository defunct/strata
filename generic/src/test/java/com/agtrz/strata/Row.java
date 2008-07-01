/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.util.ArrayList;
import java.util.List;

public class Row
{
    private final List<Comparable<?>> objects;
    
    public Row()
    {
        this.objects = new ArrayList<Comparable<?>>();
    }

    public Row(Comparable<?>... fields)
    {
        this.objects = new ArrayList<Comparable<?>>();
        for (Comparable<?> field : fields)
        {
            objects.add(field);
        }
    }
    
    public <T extends Comparable<?>> void add(T thing)
    {
        objects.add(thing);
        Comparable<Object> snert = null;
        snert.compareTo((Object) thing);
    }
    
    public <T> T get(Class<T> klass, int i)
    {
        return klass.cast(objects.get(i));
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
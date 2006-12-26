package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.swag.trace.ListFreezer;
import com.agtrz.swag.trace.NullFreezer;
import com.agtrz.swag.trace.Tracer;
import com.agtrz.swag.trace.TracerFactory;

public class LeafTier
implements Tier
{
    private final static Tracer TRACER = TracerFactory.INSTANCE.getTracer(Tier.class);

    private final int size;

    final List listOfObjects;

    LeafTier nextLeafTier;

    public LeafTier(int size)
    {
        this.size = size;
        this.listOfObjects = new ArrayList(size);
    }

    public LeafTier(int size, List listOfObjects)
    {
        this.size = size;
        this.listOfObjects = new ArrayList(listOfObjects);
    }

    public void clear()
    {
        listOfObjects.clear();
    }

    public boolean isFull()
    {
        return listOfObjects.size() == size + 1;
    }

    public Split split(Comparator comparator)
    {
        // This method actually throws away the current tier.
        if (listOfObjects.size() != size)
        {
            throw new IllegalStateException();
        }

        int middle = size >> 1;
        boolean odd = (size & 1) == 1;
        int lesser = middle - 1;
        int greater = odd ? middle + 1 : middle;
        
        Object candidate = listOfObjects.get(middle);
        int partition = -1;
        
        for (int i = 0; partition == -1 && i < middle; i++)
        {
            if (comparator.compare(candidate, listOfObjects.get(lesser)) != 0)
            {
                partition = lesser + 1;
            }
            else if (comparator.compare(candidate, listOfObjects.get(greater)) != 0)
            {
                partition = greater;
            }
            lesser--;
            greater++;
        }

        if (partition == -1)
        {
            if (nextLeafTier == null)
            {
                
            }
            else
            {
                
            }
            if (comparator.compare(nextLeafTier.listOfObjects.get(0), candidate) == 0)
            {
                
            }
        }

        List listOfLeft = listOfObjects.subList(0, partition);
        List listOfRight = listOfObjects.subList(partition, size);

        Tier left = new LeafTier(size, listOfLeft);
        Tier right = new LeafTier(size, listOfRight);

        return new Split(listOfLeft.get(listOfLeft.size() - 1), left, right);
    }

    public void insert(Comparator comparator, Object object)
    {
        if (listOfObjects.size() == size)
        {
            throw new IllegalStateException();
        }

        ListIterator objects = listOfObjects.listIterator();
        while (objects.hasNext())
        {
            Object before = objects.next();
            if (comparator.compare(object, before) <= 0)
            {
                objects.previous();
                objects.add(object);
                break;
            }
        }

        if (!objects.hasNext())
        {
            objects.previous();
            objects.add(object);
        }
    }

    public Collection find(Comparator comparator, Object object)
    {
        for (int i = 0; i < listOfObjects.size(); i++)
        {
            Object before = listOfObjects.get(i);
            if (comparator.compare(object, before) == 0)
            {
                return new LeafCollection(this, i, new Curry(new Equals(comparator), object));
            }
        }

        return Collections.EMPTY_LIST;
    }

    public boolean isLeaf()
    {
        return true;
    }

    public void copacetic(Strata.Copacetic copacetic)
    {
        TRACER.debug().record(listOfObjects, new ListFreezer(NullFreezer.INSTANCE));
        if (listOfObjects.size() < 2)
        {
            throw new IllegalStateException();
        }
        Comparator comparator = copacetic.getComparator();
        Object previous = null;
        Iterator objects = listOfObjects.iterator();
        while (objects.hasNext())
        {
            Object object = objects.next();
            if (previous != null && comparator.compare(previous, object) > 0)
            {
                throw new IllegalStateException();
            }
            previous = object;
        }
    }

    public String toString()
    {
        return listOfObjects.toString();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
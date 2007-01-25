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

    private final Comparator comparator;

    final List listOfObjects;

    LeafTier nextLeafTier;

    public LeafTier(Comparator comparator, int size)
    {
        this.size = size;
        this.comparator = comparator;
        this.listOfObjects = new ArrayList(size);
    }

    public LeafTier(Comparator comparator, int size, List listOfObjects)
    {
        this.size = size;
        this.comparator = comparator;
        this.listOfObjects = new ArrayList(listOfObjects);
    }

    public void clear()
    {
        listOfObjects.clear();
    }

    public boolean isFull()
    {
        return listOfObjects.size() == size;
    }

    public Split split(Object object)
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

        Split split = null;
        if (partition == -1)
        {
            Object repeated = listOfObjects.get(0);
            int compare = comparator.compare(object, repeated);
            if (compare == 0)
            {
                split = null; // TODO For sake of breakpoint.
            }
            else if (compare < 0)
            {
                LeafTier left = new LeafTier(comparator, size, new ArrayList());
                
                left.nextLeafTier = this;
                
                split = new Split(object, left, this);
            }
            else
            {
                LeafTier last = this;
                while (!endOfList(repeated, last))
                {
                    last = last.nextLeafTier;
                }

                LeafTier right = new LeafTier(comparator, size, new ArrayList());
                right.nextLeafTier = last.nextLeafTier;
                last.nextLeafTier = right;
                
                split = new Split(repeated, this, right);
            }
        }
        else
        {
            List listOfLeft = listOfObjects.subList(0, partition);
            List listOfRight = listOfObjects.subList(partition, size);

            LeafTier left = new LeafTier(comparator, size, listOfLeft);
            LeafTier right = new LeafTier(comparator, size, listOfRight);

            left.nextLeafTier = right;

            split = new Split(listOfLeft.get(listOfLeft.size() - 1), left, right);
        }

        return split;
    }

    private boolean endOfList(Object object, LeafTier last)
    {
        return last.nextLeafTier == null || comparator.compare(last.nextLeafTier.listOfObjects.get(0), object) != 0;
    }

    private void ensureNextLeafTier(Object object)
    {
        if (nextLeafTier == null || comparator.compare(listOfObjects.get(0), nextLeafTier.listOfObjects.get(0)) != 0)
        {
            LeafTier newNextLeafTier = new LeafTier(comparator, size);
            newNextLeafTier.nextLeafTier = nextLeafTier;
            nextLeafTier = newNextLeafTier;
        }
    }

    public void append(Object object)
    {
        if (listOfObjects.size() == size)
        {
            ensureNextLeafTier(object);
            nextLeafTier.append(object);
        }
        else
        {
            listOfObjects.add(object);
        }
    }

    public void insert(Object object)
    {
        if (listOfObjects.size() == size)
        {
            ensureNextLeafTier(object);
            nextLeafTier.append(object);
        }
        else
        {
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

                listOfObjects.add(object);
            }
        }
    }

    public Collection find(Object object)
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
        if (listOfObjects.size() < 1)
        {
            throw new IllegalStateException();
        }
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
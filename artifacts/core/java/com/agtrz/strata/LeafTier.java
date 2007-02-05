package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.agtrz.operators.Curry;
import com.agtrz.operators.Equals;
import com.agtrz.swag.util.Equator;

public abstract class LeafTier
implements Tier
{
    protected final Storage storage;

    // private final LeafPage page;

    // private final static Tracer TRACER =
    // TracerFactory.INSTANCE.getTracer(Tier.class);

    public LeafTier(Storage storage)
    {
        // this.page = storage.getPager().newLeafPage(storage);
        this.storage = storage;
    }

    // public int getSize()
    // {
    // // FIXME Include count of all linked leaves.
    // return page.getSize();
    // }

    public boolean isFull()
    {
        return getSize() == storage.getSize();
    }

    public Split split(Object object, Object keyOfObject)
    {
        if (!isFull())
        {
            throw new IllegalStateException();
        }

        int middle = storage.getSize() >> 1;
        boolean odd = (storage.getSize() & 1) == 1;
        int lesser = middle - 1;
        int greater = odd ? middle + 1 : middle;

        Object candidate = get(middle);
        int partition = -1;

        Comparator comparator = storage.getComparator();
        for (int i = 0; partition == -1 && i < middle; i++)
        {
            if (comparator.compare(candidate, get(lesser)) != 0)
            {
                partition = lesser + 1;
            }
            else if (comparator.compare(candidate, get(greater)) != 0)
            {
                partition = greater;
            }
            lesser--;
            greater++;
        }

        Pager pager = storage.getPager();
        Split split = null;
        if (partition == -1)
        {
            Object repeated = get(0);
            Object keyOfRepeated = getKey(0);
            int compare = comparator.compare(object, repeated);
            if (compare == 0)
            {
                split = null; // TODO For sake of breakpoint.
            }
            else if (compare < 0)
            {
                LeafTier right = storage.getPager().newLeafPage(storage);
                while (getSize() != 0)
                {
                    right.add(remove(0));
                }

                LeafTier next = (LeafTier) pager.getLeafPageLoader().load(storage, getNextLeafTier());

                right.setNextLeafTier(getNextLeafTier());
                setNextLeafTier(right.getKey());

                next.setPreviousLeafTier(right.getKey());
                right.setPreviousLeafTier(getKey());

                split = new Split(object, keyOfObject, right);
            }
            else
            {
                LeafTier last = this;
                while (!endOfList(repeated, last))
                {
                    last = last.getNext();
                }

                LeafTier right = pager.newLeafPage(storage);

                last.setNextLeafTier(right.getKey());
                right.setNextLeafTier(last.getNextLeafTier());

                right.getNext().setPreviousLeafTier(right.getKey());
                right.setPreviousLeafTier(last.getKey());

                split = new Split(repeated, keyOfRepeated, right);
            }
        }
        else
        {
            LeafTier right = pager.newLeafPage(storage);

            for (int i = partition; i < getSize(); i++)
            {
                right.add(remove(partition));
            }

            Object keyOfNextLeafTier = getNextLeafTier();
            setNextLeafTier(right.getKey());
            right.setNextLeafTier(keyOfNextLeafTier);

            right.getNext().setPreviousLeafTier(right.getKey());
            right.setPreviousLeafTier(getKey());

            split = new Split(get(getSize() - 1), getKey(getSize() - 1), right);
        }

        return split;
    }

     private LeafTier getPrevious()
    {
        return (LeafTier) storage.getPager().getLeafPageLoader().load(storage, getPreviousLeafTier());
    }

    private LeafTier getNext()
    {
        return (LeafTier) storage.getPager().getLeafPageLoader().load(storage, getNextLeafTier());
    }

    private boolean endOfList(Object object, LeafTier last)
    {
        return storage.getPager().isKeyNull(getNextLeafTier()) || storage.getComparator().compare(last.getNext().get(0), object) != 0;
    }

    private void ensureNextLeafTier(Object object)
    {
        Pager pager = storage.getPager();
        if (pager.isKeyNull(getNextLeafTier()) || storage.getComparator().compare(get(0), getNext().get(0)) != 0)
        {
            LeafTier newNextLeafTier = pager.newLeafPage(storage);

            newNextLeafTier.setNextLeafTier(getNextLeafTier());
            setNextLeafTier(newNextLeafTier.getKey());

            getNext().setPreviousLeafTier(newNextLeafTier.getKey());
            newNextLeafTier.setPreviousLeafTier(getKey());
        }
    }

    public void append(Object object)
    {
        if (getSize() == storage.getSize())
        {
            ensureNextLeafTier(object);
            getNext().append(object);
        }
        else
        {
            add(object);
        }
    }

    public void insert(Object object)
    {
        if (getSize() == storage.getSize())
        {
            ensureNextLeafTier(object);
            getNext().append(object);
        }
        else
        {
            Comparator comparator = storage.getComparator();
            ListIterator objects = listIterator();
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
                add(object);
            }
        }
    }

    public Collection find(Object object)
    {
        Comparator comparator = storage.getComparator();
        for (int i = 0; i < getSize(); i++)
        {
            Object before = get(i);
            if (comparator.compare(object, before) == 0)
            {
                return new LeafCollection(storage, this, i, new Curry(new Equals(comparator), object));
            }
        }

        return Collections.EMPTY_LIST;
    }

    /**
     * Remove all objects that match the object according to the equator,
     * while updating the inner tree if the object removed was used as a pivot
     * in the inner tree.
     * 
     * @param toRemove
     *            An object that represents the objects that will be removed
     *            from the B+Tree.
     * @param equator
     *            The comparison logic that will determine of an object in the
     *            leaf is equal to the specified object.
     * @return A colletion of the objects removed from the tree.
     */
    public Collection remove(Object toRemove, Equator equator)
    {
        List listOfRemoved = new ArrayList();
        Iterator objects = listIterator();
        while (objects.hasNext())
        {
            Object candidate = objects.next();
            if (equator.equals(candidate, toRemove))
            {
                listOfRemoved.add(candidate);
                objects.remove();
            }
        }
        LeafTier lastLeafTier = this;
        LeafTier leafTier = getNext();
        while (equator.equals(leafTier.get(0), toRemove))
        {
            objects = leafTier.listIterator();
            while (objects.hasNext())
            {
                Object candidate = objects.next();
                if (equator.equals(candidate, toRemove))
                {
                    listOfRemoved.add(candidate);
                    objects.remove();
                }
            }
            if (leafTier.getSize() == 0)
            {
                lastLeafTier.setNextLeafTier(leafTier.getNextLeafTier());
                leafTier.getNext().setPreviousLeafTier(lastLeafTier.getKey());
            }
            leafTier = leafTier.getNext();
        }
        return listOfRemoved;
    }

    public boolean isLeaf()
    {
        return true;
    }

    public void copacetic(Strata.Copacetic copacetic)
    {
        if (getSize() < 1)
        {
            throw new IllegalStateException();
        }
        Object previous = null;
        Iterator objects = listIterator();
        Comparator comparator = storage.getComparator();
        while (objects.hasNext())
        {
            Object object = objects.next();
            if (previous != null && comparator.compare(previous, object) > 0)
            {
                throw new IllegalStateException();
            }
            previous = object;
        }
        if (!storage.getPager().isKeyNull(getNextLeafTier()) && comparator.compare(get(getSize() - 1), getNext().get(0)) == 0 && storage.getSize() != getSize() && comparator.compare(get(0), get(getSize() - 1)) != 0)
        {
            throw new IllegalStateException();
        }
    }

    public void consume(Tier left, Object key)
    {
        LeafTier leafTier = (LeafTier) left;

        setPreviousLeafTier(leafTier.getPreviousLeafTier());
        getPrevious().setNextLeafTier(getKey());

        while (leafTier.getSize() != 0)
        {
            shift(leafTier.remove(0));
        }
    }

    public abstract Object getKey(int index);

    public abstract Object get(int index);

    public abstract Object remove(int index);

    public abstract void add(Object object);
    
    public abstract void shift(Object object);

    public abstract ListIterator listIterator();

    // FIXME Rename.
    public abstract Object getPreviousLeafTier();

    public abstract void setPreviousLeafTier(Object keyOfPreviousLeafTier);

    public abstract Object getNextLeafTier();

    public abstract void setNextLeafTier(Object keyOfNextLeafTier);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
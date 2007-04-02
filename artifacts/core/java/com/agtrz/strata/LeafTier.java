package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public class LeafTier
implements Tier
{
    private Object addressOfNext;

    private Object addressOfPrevious;

    private final Object storageData;

    private final List listOfObjects;

    protected final Strata.Structure structure;

    public LeafTier(Strata.Structure structure, Object storageData)
    {
        this.structure = structure;
        this.storageData = storageData;
        this.listOfObjects = new ArrayList(structure.getSize());
    }

    public Object getKey()
    {
        return structure.getStorage().getKey(this);
    }

    public boolean isFull()
    {
        return getSize() == structure.getSize();
    }

    public Split split(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
    {
        if (!isFull())
        {
            throw new IllegalStateException();
        }

        int middle = structure.getSize() >> 1;
        boolean odd = (structure.getSize() & 1) == 1;
        int lesser = middle - 1;
        int greater = odd ? middle + 1 : middle;

        int partition = -1;

        Strata.Criteria candidate = structure.getCriterion().newCriteria(txn, get(middle));
        for (int i = 0; partition == -1 && i < middle; i++)
        {
            if (candidate.partialMatch(get(lesser)) != 0)
            {
                partition = lesser + 1;
            }
            else if (candidate.partialMatch(get(greater)) != 0)
            {
                partition = greater;
            }
            lesser--;
            greater++;
        }

        Storage storage = structure.getStorage();
        Split split = null;
        if (partition == -1)
        {
            Object repeated = get(0);
            int compare = criteria.partialMatch(repeated);
            if (compare < 0)
            {
                LeafTier right = storage.newLeafTier(structure, txn);
                while (getSize() != 0)
                {
                    right.add(remove(0));
                }

                link(txn, this, right, setOfDirty);

                split = new Split(criteria.getObject(), right);
            }
            else if (compare > 0)
            {
                LeafTier last = this;
                while (!endOfList(txn, repeated, last))
                {
                    last = last.getNext(txn);
                }

                LeafTier right = storage.newLeafTier(structure, txn);
                link(txn, last, right, setOfDirty);

                split = new Split(repeated, right);
            }
        }
        else
        {
            LeafTier right = storage.newLeafTier(structure, txn);

            while (partition != getSize())
            {
                right.add(remove(partition));
            }

            link(txn, this, right, setOfDirty);

            split = new Split(get(getSize() - 1), right);
        }

        return split;
    }

    public void append(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
    {
        if (getSize() == structure.getSize())
        {
            ensureNextLeafTier(txn, criteria, setOfDirty);
            getNext(txn).append(txn, criteria, setOfDirty);
        }
        else
        {
            add(criteria.getObject());
            setOfDirty.add(this);
        }
    }

    public void insert(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
    {
        if (getSize() == structure.getSize())
        {
            ensureNextLeafTier(txn, criteria, setOfDirty);
            getNext(txn).append(txn, criteria, setOfDirty);
        }
        else
        {
            ListIterator objects = listIterator();
            while (objects.hasNext())
            {
                Object before = objects.next();
                if (criteria.partialMatch(before) <= 0)
                {
                    objects.previous();
                    objects.add(criteria.getObject());
                    break;
                }
            }

            if (!objects.hasNext())
            {
                add(criteria.getObject());
            }

            setOfDirty.add(this);
        }
    }

    public Strata.Cursor find(Object txn, Strata.Criteria criteria)
    {
        for (int i = 0; i < getSize(); i++)
        {
            Object before = get(i);
            if (criteria.partialMatch(before) == 0)
            {
                return new Strata.ForwardCursor(structure, txn, this, i);
            }
        }
        return Strata.EMPTY_CURSOR;
    }

    /**
     * Remove all objects that match the object according to the equator,
     * while updating the inner tree if the object removed was used as a pivot
     * in the inner tree.
     * 
     * @param criteria
     *            An object that represents the objects that will be removed
     *            from the B+Tree.
     * @param equator
     *            The comparison logic that will determine of an object in the
     *            leaf is equal to the specified object.
     * @return A colletion of the objects removed from the tree.
     */
    public Collection remove(Object txn, Strata.Criteria criteria)
    {
        List listOfRemoved = new ArrayList();
        Iterator objects = listIterator();
        while (objects.hasNext())
        {
            Object candidate = objects.next();
            if (criteria.exactMatch(candidate))
            {
                listOfRemoved.add(candidate);
                objects.remove();
            }
        }
        Storage storage = structure.getStorage();
        LeafTier lastLeaf = this;
        LeafTier leaf = null;
        while (!storage.isKeyNull(lastLeaf.getNextLeafKey()) && criteria.exactMatch((leaf = lastLeaf.getNext(txn)).get(0)))
        {
            objects = leaf.listIterator();
            while (objects.hasNext())
            {
                Object candidate = objects.next();
                if (criteria.equals(candidate))
                {
                    listOfRemoved.add(candidate);
                    objects.remove();
                }
            }
            if (leaf.getSize() == 0)
            {
                lastLeaf.setNextLeafKey(leaf.getNextLeafKey());
                leaf.getNext(txn).setPreviousLeafKey(lastLeaf.getKey());
            }
            lastLeaf = leaf;
        }
        return listOfRemoved;
    }

    public void consume(Object txn, Tier left, Strata.TierSet setOfDirty)
    {
        LeafTier leaf = (LeafTier) left;

        setPreviousLeafKey(leaf.getPreviousLeafKey());
        if (!structure.getStorage().isKeyNull(getPreviousLeafKey()))
        {
            getPrevious(txn).setNextLeafKey(getKey());
        }

        while (leaf.getSize() != 0)
        {
            shift(leaf.remove(leaf.getSize() - 1));
        }

        setOfDirty.add(this);

        structure.getStorage().free(structure, txn, leaf);
    }

    public void revert(Object txn)
    {
        structure.getStorage().revert(structure, txn, this);
    }

    public void write(Object txn)
    {
        structure.getStorage().write(structure, txn, this);
    }

    public void copacetic(Object txn, Strata.Copacetic copacetic)
    {
        if (getSize() < 1)
        {
            throw new IllegalStateException();
        }
        Object previous = null;
        Iterator objects = listIterator();
        Comparator comparator = structure.newComparator(txn);
        while (objects.hasNext())
        {
            Object object = objects.next();
            if (previous != null && comparator.compare(previous, object) > 0)
            {
                throw new IllegalStateException();
            }
            previous = object;
        }
        if (!structure.getStorage().isKeyNull(getNextLeafKey()) && comparator.compare(get(getSize() - 1), getNext(txn).get(0)) == 0 && structure.getSize() != getSize() && comparator.compare(get(0), get(getSize() - 1)) != 0)
        {
            throw new IllegalStateException();
        }
    }

    private LeafTier getPrevious(Object txn)
    {
        return (LeafTier) structure.getStorage().getLeafTier(structure, txn, getPreviousLeafKey());
    }

    private LeafTier getNext(Object txn)
    {
        return (LeafTier) structure.getStorage().getLeafTier(structure, txn, getNextLeafKey());
    }

    private boolean endOfList(Object txn, Object object, LeafTier last)
    {
        return structure.getStorage().isKeyNull(last.getNextLeafKey()) || structure.newComparator(txn).compare(last.getNext(txn).get(0), object) != 0;
    }

    private void ensureNextLeafTier(Object txn, Strata.Criteria criteria, Strata.TierSet setOfDirty)
    {
        Storage storage = structure.getStorage();
        if (storage.isKeyNull(getNextLeafKey()) || criteria.partialMatch(getNext(txn).get(0)) != 0)
        {
            LeafTier nextLeaf = storage.newLeafTier(structure, txn);
            link(txn, this, nextLeaf, setOfDirty);
        }
    }

    private void link(Object txn, LeafTier leaf, LeafTier nextLeaf, Strata.TierSet setOfDirty)
    {
        setOfDirty.add(leaf);
        setOfDirty.add(nextLeaf);
        Object nextLeafKey = leaf.getNextLeafKey();
        leaf.setNextLeafKey(nextLeaf.getKey());
        nextLeaf.setNextLeafKey(nextLeafKey);
        if (!structure.getStorage().isKeyNull(nextLeafKey))
        {
            LeafTier next = nextLeaf.getNext(txn);
            setOfDirty.add(next);
            next.setPreviousLeafKey(nextLeaf.getKey());
        }
        nextLeaf.setPreviousLeafKey(leaf.getKey());
    }

    public Object getStorageData()
    {
        return storageData;
    }

    public int getSize()
    {
        return listOfObjects.size();
    }

    public Object get(int index)
    {
        return listOfObjects.get(index);
    }

    public String toString()
    {
        return listOfObjects.toString();
    }

    public Object remove(int index)
    {
        return listOfObjects.remove(index);
    }

    public void add(Object object)
    {
        listOfObjects.add(object);
    }

    public void shift(Object object)
    {
        listOfObjects.add(0, object);
    }

    public ListIterator listIterator()
    {
        return listOfObjects.listIterator();
    }

    public Object getPreviousLeafKey()
    {
        return addressOfPrevious;
    }

    public void setPreviousLeafKey(Object previousLeafKey)
    {
        this.addressOfPrevious = previousLeafKey;
    }

    public Object getNextLeafKey()
    {
        return addressOfNext;
    }

    public void setNextLeafKey(Object nextLeafKey)
    {
        this.addressOfNext = nextLeafKey;
    }

}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public abstract class LeafTier
implements Tier
{
    protected final Strata.Structure structure;

    // private final LeafPage page;

    // private final static Tracer TRACER =
    // TracerFactory.INSTANCE.getTracer(Tier.class);

    public LeafTier(Strata.Structure structure)
    {
        // this.page = storage.getPager().newLeafPage(storage);
        this.structure = structure;
    }

    // public int getSize()
    // {
    // // FIXME Include count of all linked leaves.
    // return page.getSize();
    // }

    public boolean isFull()
    {
        return getSize() == structure.getSize();
    }

    public Split split(Object txn, Strata.Criteria criteria)
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

        Strata.Criteria candidate = structure.getCriterion().newCriteria(get(middle));
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

        Storage pager = structure.getStorage();
        Split split = null;
        if (partition == -1)
        {
            Object repeated = get(0);
            int compare = criteria.partialMatch(repeated);
            if (compare == 0)
            {
                split = null; // TODO For sake of breakpoint.
            }
            else if (compare < 0)
            {
                LeafTier right = structure.getStorage().newLeafPage(structure, txn);
                while (getSize() != 0)
                {
                    right.add(remove(0));
                }

                LeafTier next = (LeafTier) pager.getLeafPageLoader().load(structure, txn, getNextLeafTier());

                right.setNextLeafTier(getNextLeafTier());
                setNextLeafTier(right.getKey());

                next.setPreviousLeafTier(right.getKey());
                right.setPreviousLeafTier(getKey());

                split = new Split(criteria.getObject(), right);
            }
            else
            {
                LeafTier last = this;
                while (!endOfList(txn, repeated, last))
                {
                    last = last.getNext(txn);
                }

                LeafTier right = pager.newLeafPage(structure, txn);

                last.setNextLeafTier(right.getKey());
                right.setNextLeafTier(last.getNextLeafTier());

                right.getNext(txn).setPreviousLeafTier(right.getKey());
                right.setPreviousLeafTier(last.getKey());

                split = new Split(repeated, right);
            }
        }
        else
        {
            LeafTier right = pager.newLeafPage(structure, txn);

            for (int i = partition; i < getSize(); i++)
            {
                right.add(remove(partition));
            }

            Object keyOfNextLeafTier = getNextLeafTier();
            setNextLeafTier(right.getKey());
            right.setNextLeafTier(keyOfNextLeafTier);

            right.getNext(txn).setPreviousLeafTier(right.getKey());
            right.setPreviousLeafTier(getKey());

            split = new Split(get(getSize() - 1), right);
        }

        return split;
    }

    private LeafTier getPrevious(Object txn)
    {
        return (LeafTier) structure.getStorage().getLeafPageLoader().load(structure, txn, getPreviousLeafTier());
    }

    private LeafTier getNext(Object txn)
    {
        return (LeafTier) structure.getStorage().getLeafPageLoader().load(structure, txn, getNextLeafTier());
    }

    private boolean endOfList(Object txn, Object object, LeafTier last)
    {
        return structure.getStorage().isKeyNull(getNextLeafTier()) || structure.getCriterion().newCriteria(last.getNext(txn).get(0)).partialMatch(object) != 0;
    }

    private void ensureNextLeafTier(Object txn, Strata.Criteria criteria)
    {
        Storage pager = structure.getStorage();
        if (pager.isKeyNull(getNextLeafTier()) || criteria.partialMatch(getNext(txn).get(0)) != 0)
        {
            LeafTier newNextLeafTier = pager.newLeafPage(structure, txn);

            newNextLeafTier.setNextLeafTier(getNextLeafTier());
            setNextLeafTier(newNextLeafTier.getKey());

            getNext(txn).setPreviousLeafTier(newNextLeafTier.getKey());
            newNextLeafTier.setPreviousLeafTier(getKey());
        }
    }

    public void append(Object txn, Strata.Criteria criteria)
    {
        if (getSize() == structure.getSize())
        {
            ensureNextLeafTier(txn, criteria);
            getNext(txn).append(txn, criteria);
        }
        else
        {
            add(criteria.getObject());
        }
    }

    public void insert(Object txn, Strata.Criteria criteria)
    {
        if (getSize() == structure.getSize())
        {
            ensureNextLeafTier(txn, criteria);
            getNext(txn).append(txn, criteria);
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
        }
    }

    public Collection find(Object txn, Strata.Criteria criteria)
    {
        for (int i = 0; i < getSize(); i++)
        {
            Object before = get(i);
            if (criteria.partialMatch(before) == 0)
            {
                return new LeafCollection(structure, txn, this, i, criteria);
            }
        }

        return Collections.EMPTY_LIST;
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
            if (criteria.equals(candidate))
            {
                listOfRemoved.add(candidate);
                objects.remove();
            }
        }
        LeafTier lastLeafTier = this;
        LeafTier leafTier = getNext(txn);
        while (criteria.equals(leafTier.get(0)))
        {
            objects = leafTier.listIterator();
            while (objects.hasNext())
            {
                Object candidate = objects.next();
                if (criteria.equals(candidate))
                {
                    listOfRemoved.add(candidate);
                    objects.remove();
                }
            }
            if (leafTier.getSize() == 0)
            {
                lastLeafTier.setNextLeafTier(leafTier.getNextLeafTier());
                leafTier.getNext(txn).setPreviousLeafTier(lastLeafTier.getKey());
            }
            leafTier = leafTier.getNext(txn);
        }
        return listOfRemoved;
    }

    public boolean isLeaf()
    {
        return true;
    }

    public void copacetic(Object txn, Strata.Copacetic copacetic)
    {
        if (getSize() < 1)
        {
            throw new IllegalStateException();
        }
        Object previous = null;
        Iterator objects = listIterator();
        while (objects.hasNext())
        {
            Object object = objects.next();
            if (previous != null && structure.getCriterion().newCriteria(previous).partialMatch(object) > 0)
            {
                throw new IllegalStateException();
            }
            previous = object;
        }
        if (!structure.getStorage().isKeyNull(getNextLeafTier()) && structure.getCriterion().newCriteria(get(getSize() - 1)).partialMatch(getNext(txn).get(0)) == 0 && structure.getSize() != getSize() && structure.getCriterion().newCriteria(get(0)).partialMatch(get(getSize() - 1)) != 0)
        {
            throw new IllegalStateException();
        }
    }

    public void consume(Object txn, Tier left, Object key)
    {
        LeafTier leafTier = (LeafTier) left;

        setPreviousLeafTier(leafTier.getPreviousLeafTier());
        getPrevious(txn).setNextLeafTier(getKey());

        while (leafTier.getSize() != 0)
        {
            shift(leafTier.remove(0));
        }
    }

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
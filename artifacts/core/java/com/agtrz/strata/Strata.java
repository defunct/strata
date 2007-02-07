package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Strata
{
    public final static Cursor EMPTY_CURSOR = new EmptyCursor(true);

    private final Structure structure;

    private final InnerTier root;

    private int size;

    public Strata()
    {
        this(new ArrayListStorage(), null, new BasicCriteriaServer(), 5);
    }

    public Strata(Storage storage, Object txn, CriteriaServer criterion, int size)
    {
        this.structure = new Structure(storage, criterion, size);
        this.root = structure.getStorage().newInnerTier(structure, txn, Tier.LEAF);
        this.root.add(new Branch(structure.getStorage().newLeafTier(structure, txn).getKey(), Branch.TERMINAL, 0));
    }

    public int getSize()
    {
        return size;
    }

    public Query query(Object txn)
    {
        return new Query(txn);
    }

    public final static class Creator
    {
        private CriteriaServer criterion = new BasicCriteriaServer();

        private Storage storage = new ArrayListStorage();

        private int size = 5;

        public Strata create(Object txn)
        {
            return new Strata(storage, txn, criterion, size);
        }

        public void setCriteriaServer(CriteriaServer criterion)
        {
            this.criterion = criterion;
        }

        public void setStorage(Storage storage)
        {
            this.storage = storage;
        }

        public void setSize(int size)
        {
            this.size = size;
        }
    }

    public static class Structure
    implements Comparator
    {
        private final Storage storage;

        private final CriteriaServer criterion;

        private final int size;

        public Structure(Storage storage, CriteriaServer criterion, int size)
        {
            this.storage = storage;
            this.criterion = criterion;
            this.size = size;
        }

        public Storage getStorage()
        {
            return storage;
        }

        public CriteriaServer getCriterion()
        {
            return criterion;
        }

        public int getSize()
        {
            return size;
        }

        public int compare(Object left, Object right)
        {
            return getCriterion().newCriteria(left).partialMatch(right);
        }
    }

    public interface Criteria
    {
        public Object getObject();

        public int partialMatch(Object object);

        public boolean exactMatch(Object object);
    }

    public interface CriteriaServer
    {
        public Criteria newCriteria(Object object);
    }

    public static class BasicCriteria
    implements Criteria
    {
        private final Comparable comparable;

        public BasicCriteria(Comparable comparable)
        {
            this.comparable = comparable;
        }

        public int partialMatch(Object object)
        {
            return comparable.compareTo(object);
        }

        public boolean exactMatch(Object object)
        {
            return comparable.equals(object);
        }

        public Object getObject()
        {
            return comparable;
        }
    }

    public static class BasicCriteriaServer
    implements CriteriaServer
    {
        public Criteria newCriteria(Object object)
        {
            return new BasicCriteria((Comparable) object);
        }
    }

    public final class Query
    {
        private final Object txn;

        public Query(Object txn)
        {
            this.txn = txn;
        }

        public int getSize()
        {
            return size;
        }

        public void insert(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(object);

            // Maintaining a list of tiers to split.
            //
            // During the decent into the tree, we check for full tiers. When
            // a full tier is first encountered, a list of full tiers is begun
            // starting with the parent of the first full tier, and followed
            // by all of the full tiers to the leaf tier. If less than full
            // tier is encountered, we reset the list of tiers.
            //
            // The list of tiers is locked in order from upper most teirs to
            // the leaf.

            List listOfFullTiers = new ArrayList();
            InnerTier grandParent = null;
            InnerTier parent = null;
            InnerTier inner = root;

            if (inner.isFull())
            {
                listOfFullTiers.add(grandParent);
                listOfFullTiers.add(parent);
            }

            for (;;)
            {
                grandParent = parent;
                parent = inner;
                Branch branch = inner.find(criteria);
                Tier tier = parent.load(txn, branch.getLeftKey());

                if (tier.isFull())
                {
                    if (listOfFullTiers.isEmpty())
                    {
                        listOfFullTiers.add(grandParent);
                    }
                    listOfFullTiers.add(parent);
                }
                else
                {
                    listOfFullTiers.clear();
                }

                if (parent.getChildType() == Tier.LEAF)
                {
                    if (tier.isFull())
                    {
                        listOfFullTiers.add(tier);

                        Iterator ancestors = listOfFullTiers.iterator();
                        InnerTier reciever = (InnerTier) ancestors.next();
                        tier = (Tier) ancestors.next();
                        do
                        {
                            parent = reciever;
                            reciever = (InnerTier) tier;
                            Tier full = (Tier) ancestors.next();
                            Split split = full.split(txn, criteria);
                            if (split == null)
                            {
                                tier = full;
                            }
                            else
                            {
                                if (reciever == null)
                                {
                                    reciever = (InnerTier) full;
                                    reciever.splitRootTier(txn, split);
                                }
                                else
                                {
                                    reciever.replace(full, split);
                                    if (parent != null)
                                    {
                                        parent.find(criteria).setSize(reciever.getSize());
                                    }
                                }
                                branch = reciever.find(criteria);
                                tier = reciever.load(txn, branch.getLeftKey());
                            }
                        }
                        while (ancestors.hasNext());
                    }

                    LeafTier leaf = (LeafTier) tier;
                    branch.setSize(branch.getSize() + 1);
                    leaf.insert(txn, criteria);
                    break;
                }

                inner = (InnerTier) tier;
            }

            size++;
        }

        public Cursor find(Object object)
        {
            return find(structure.getCriterion().newCriteria(object));
        }

        public Cursor find(Strata.Criteria criteria)
        {
            InnerTier tier = root;
            for (;;)
            {
                Branch branch = tier.find(criteria);
                if (tier.getChildType() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.load(txn, branch.getLeftKey());
                    return leaf.find(txn, criteria);
                }
                tier = (InnerTier) tier.load(txn, branch.getLeftKey());
            }
        }

        public Collection remove(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(object);
            LinkedList listOfAncestors = new LinkedList();
            InnerTier inner = null;
            InnerTier parent = null;
            InnerTier tier = root;
            for (;;)
            {
                listOfAncestors.addLast(tier);
                parent = tier;
                Branch branch = tier.find(criteria);
                if (branch.getObject() != Branch.TERMINAL && criteria.exactMatch(branch.getObject()))
                {
                    inner = tier;
                }
                if (tier.getChildType() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.load(txn, branch.getLeftKey());
                    Collection collection = leaf.remove(txn, criteria);
                    branch.setSize(leaf.getSize());

                    if (inner != null)
                    {
                        int objectCount = leaf.getSize();
                        if (objectCount == 0)
                        {
                            int index = parent.getIndexOfTier(leaf);
                            if (inner.getChildType() == Tier.LEAF)
                            {
                                parent.removeLeafTier(leaf);
                            }
                            else
                            {
                                Branch newPivot = parent.get(index - 1);
                                parent.removeLeafTier(leaf);
                                parent.replacePivot(structure.getCriterion().newCriteria(newPivot.getObject()), Branch.TERMINAL);
                                inner.replacePivot(criteria, newPivot.getObject());
                            }
                        }
                        else
                        {
                            Object newPivot = leaf.get(objectCount - 1);
                            inner.replacePivot(criteria, newPivot);
                        }
                    }

                    Tier child = leaf;
                    Iterator ancestors = listOfAncestors.iterator();
                    while (ancestors.hasNext())
                    {
                        InnerTier merge = (InnerTier) ancestors.next();
                        merge.merge(txn, child);
                    }

                    size -= collection.size();
                    return collection;
                }
                tier = (InnerTier) parent.load(txn, branch.getLeftKey());
            }
        }

        public Cursor values()
        {
            Branch branch = null;
            InnerTier tier = root;
            for (;;)
            {
                branch = tier.get(0);
                if (tier.getChildType() == Tier.LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.load(txn, branch.getLeftKey());
            }
            return new ForwardCursor(structure, txn, (LeafTier) tier.load(txn, branch.getLeftKey()), 0);
        }

        public void copacetic()
        {
            root.copacetic(txn, new Copacetic(structure));
            Iterator iterator = values();
            if (getSize() != 0)
            {
                Object previous = iterator.next();
                for (int i = 1; i < size; i++)
                {
                    if (!iterator.hasNext())
                    {
                        throw new IllegalStateException();
                    }
                    Object next = iterator.next();
                    if (structure.getCriterion().newCriteria(previous).partialMatch(next) > 0)
                    {
                        throw new IllegalStateException();
                    }
                    previous = next;
                }
                if (iterator.hasNext())
                {
                    throw new IllegalStateException();
                }
            }
            else if (iterator.hasNext())
            {
                throw new IllegalStateException();
            }
        }
    }

    public interface Cursor
    extends Iterator
    {
        public boolean isEmpty();

        public boolean isForward();

        public Cursor reverse();

        public Cursor newCursor();
    }

    public final static class ForwardCursor
    implements Cursor
    {
        private final Strata.Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        public ForwardCursor(Strata.Structure structure, Object txn, LeafTier leaf, int index)
        {
            this.structure = structure;
            this.txn = txn;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public boolean isForward()
        {
            return true;
        }

        public Cursor newCursor()
        {
            return new ForwardCursor(structure, txn, leaf, index);
        }

        public Cursor reverse()
        {
            return new ReverseCursor(structure, txn, leaf, index);
        }

        public boolean hasNext()
        {
            return index < leaf.getSize() || !structure.getStorage().isKeyNull(leaf.getNextLeafKey());
        }

        public Object next()
        {
            if (index == leaf.getSize())
            {
                Storage storage = structure.getStorage();
                if (storage.isKeyNull(leaf.getNextLeafKey()))
                {
                    throw new IllegalStateException();
                }
                leaf = (LeafTier) structure.getStorage().getLeafTierLoader().load(structure, txn, leaf.getNextLeafKey());
                index = 0;
            }
            return leaf.get(index++);
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public final static class ReverseCursor
    implements Cursor
    {
        private final Strata.Structure structure;

        private final Object txn;

        private int index;

        private LeafTier leaf;

        public ReverseCursor(Strata.Structure structure, Object txn, LeafTier leaf, int index)
        {
            this.structure = structure;
            this.txn = txn;
            this.leaf = leaf;
            this.index = index;
        }

        public boolean isEmpty()
        {
            return false;
        }

        public boolean isForward()
        {
            return false;
        }

        public Cursor newCursor()
        {
            return new ReverseCursor(structure, txn, leaf, index);
        }

        public Cursor reverse()
        {
            return new ForwardCursor(structure, txn, leaf, index);
        }

        public boolean hasNext()
        {
            return index > 0 || !structure.getStorage().isKeyNull(leaf.getPreviousLeafKey());
        }

        public Object next()
        {
            if (index == 0)
            {
                Storage storage = structure.getStorage();
                if (storage.isKeyNull(leaf.getPreviousLeafKey()))
                {
                    throw new IllegalStateException();
                }
                leaf = (LeafTier) structure.getStorage().getLeafTierLoader().load(structure, txn, leaf.getPreviousLeafKey());
                index = leaf.getSize();
            }
            return leaf.get(--index);
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final static class EmptyCursor
    implements Cursor
    {
        public final boolean forward;

        public EmptyCursor(boolean forward)
        {
            this.forward = forward;
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean isForward()
        {
            return forward;
        }

        public Cursor newCursor()
        {
            return new EmptyCursor(forward);
        }

        public Cursor reverse()
        {
            return new EmptyCursor(!forward);
        }

        public boolean hasNext()
        {
            return false;
        }

        public Object next()
        {
            return new IllegalStateException();
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class Copacetic
    {
        private final Set seen;

        private Copacetic(Comparator comparator)
        {
            this.seen = new TreeSet(comparator);
        }

        public boolean unique(Object object)
        {
            if (seen.contains(object))
            {
                return false;
            }
            seen.add(object);
            return true;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
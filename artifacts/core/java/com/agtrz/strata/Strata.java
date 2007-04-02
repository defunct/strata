package com.agtrz.strata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Strata
implements Serializable
{
    private static final long serialVersionUID = 20070207L;

    public final static Cursor EMPTY_CURSOR = new EmptyCursor(true);

    private final Structure structure;

    private final Object rootKey;

    private int size;

    public Strata()
    {
        this(new ArrayListStorage(), null, new BasicCriteriaServer(), 5);
    }

    public Strata(Storage storage, Object txn, CriteriaServer criterion, int size)
    {
        Structure structure = new Structure(storage, criterion, size);
        InnerTier root = structure.getStorage().newInnerTier(structure, txn, Tier.LEAF);
        LeafTier leaf = structure.getStorage().newLeafTier(structure, txn);
        leaf.write(txn);

        root.add(new Branch(leaf.getKey(), Branch.TERMINAL, 0));
        root.write(txn);

        this.structure = structure;
        this.rootKey = root.getKey();
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

        public void setStorage(Storage storage)
        {
            this.storage = storage;
        }

        public void setCriteriaServer(CriteriaServer criterion)
        {
            this.criterion = criterion;
        }

        public void setSize(int size)
        {
            this.size = size;
        }
    }

    public static class Structure
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

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

        public Comparator newComparator(Object txn)
        {
            return new CopaceticComparator(criterion, txn);
        }
    }

    private final static class CopaceticComparator
    implements Comparator
    {
        private final CriteriaServer criterion;

        private final Object txn;

        public CopaceticComparator(CriteriaServer criterion, Object txn)
        {
            this.criterion = criterion;
            this.txn = txn;
        }

        public int compare(Object left, Object right)
        {
            return criterion.newCriteria(txn, left).partialMatch(right);
        }
    }

    public interface Resolver
    {
        public Object resolve(Object txn, Object object);
    }

    public final static class BasicResolver
    implements Resolver, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Object resolve(Object txn, Object object)
        {
            return object;
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
        public Criteria newCriteria(Object txn, Object object);

        public Criteria newCriteria(Comparison comparison, Object txn, Object object);
    }

    public interface Comparison
    {
        public int partialMatch(Object criteria, Object stored);

        // FIXME This is not actually used in a query, only in insert and
        // delete.
        // FIXME getObject is only used in insert.
        // FIXME Does insert use exactMatch?
        public boolean exactMatch(Object criteria, Object stored);
    }

    public static class ComplexCriteria
    implements Criteria, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Resolver resolver;

        private final Comparison comparison;

        private final Object txn;

        private final Object object;

        private final Object criteria;

        public ComplexCriteria(Resolver resolver, Comparison comparison, Object txn, Object object)
        {
            this.resolver = resolver;
            this.comparison = comparison;
            this.txn = txn;
            this.object = object;
            this.criteria = resolver.resolve(txn, object);
        }

        public int partialMatch(Object object)
        {
            return comparison.partialMatch(criteria, resolver.resolve(txn, object));
        }

        public boolean exactMatch(Object object)
        {
            return comparison.exactMatch(criteria, resolver.resolve(txn, object));
        }

        public Object getObject()
        {
            return object; // FIXME Broken. Why is it not used?
        }
    }

    public static class BasicComparison
    implements Comparison, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        protected int partialMatch(Comparable[] left, Comparable[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                int compare = left[i].compareTo(right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return left.length - right.length;
        }

        public int partialMatch(Object criteria, Object stored)
        {
            return ((Comparable) criteria).compareTo(stored);
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            return criteria.equals(stored);
        }
    };

    public interface FieldExtractor
    {
        public Object[] getFields(Object object);
    }

    public static class FieldComparison
    implements Comparison, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final FieldExtractor storedFields;

        private final FieldExtractor soughtFields;

        public FieldComparison(FieldExtractor soughtFields, FieldExtractor storedFields)
        {
            this.soughtFields = soughtFields;
            this.storedFields = storedFields;
        }

        public int partialMatch(Object criteria, Object stored)
        {
            return partialMatch(soughtFields.getFields(criteria), storedFields.getFields(stored));
        }

        public boolean exactMatch(Object criteria, Object stored)
        {
            return exactMatch(soughtFields.getFields(criteria), storedFields.getFields(stored));
        }

        protected int partialMatch(Object[] left, Object[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                int compare = ((Comparable) left[i]).compareTo(right[i]);
                if (compare != 0)
                {
                    return compare;
                }
            }
            return left.length - right.length;
        }

        protected boolean exactMatch(Object[] left, Object[] right)
        {
            for (int i = 0; i < left.length && i < right.length; i++)
            {
                if (left[i].equals(right[i]))
                {
                    return false;
                }
            }
            return left.length == right.length;
        }
    };

    public static class ComplexCriteriaServer
    implements CriteriaServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Resolver resolver;

        private final Comparison comparison;

        public ComplexCriteriaServer(Resolver resolver, Comparison comparison)
        {
            this.resolver = resolver;
            this.comparison = comparison;
        }

        public Criteria newCriteria(Object txn, Object object)
        {
            return new ComplexCriteria(resolver, comparison, txn, object);
        }

        public Criteria newCriteria(Comparison comparison, Object txn, Object object)
        {
            return new ComplexCriteria(resolver, comparison, txn, object);
        }
    }

    // FIXME When is this used and how? Becomming moot? New find abstracts,
    // hides enough.
    public static class BasicCriteria
    implements Criteria, Serializable
    {
        private static final long serialVersionUID = 20070208L;

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
    implements CriteriaServer, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        public Criteria newCriteria(Object txn, Object object)
        {
            return new BasicCriteria((Comparable) object);
        }

        public Criteria newCriteria(Comparison comparison, Object txn, Object object)
        {
            return new ComplexCriteria(new Strata.BasicResolver(), comparison, null, object);
        }
    }

    public final class TierSet
    {
        private final Map mapOfTiers;

        public TierSet(Map mapOfTiers)
        {
            this.mapOfTiers = mapOfTiers;
        }

        public void add(Tier tier)
        {
            mapOfTiers.put(tier.getKey(), tier);
        }
    }

    public final class Query
    {
        private final Object txn;

        private final Map mapOfDirtyTiers;

        public Query(Object txn)
        {
            this.txn = txn;
            this.mapOfDirtyTiers = new HashMap();
        }

        public int getSize()
        {
            return size;
        }

        public void insert(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(txn, object);

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
            InnerTier inner = getRoot();

            if (inner.isFull())
            {
                listOfFullTiers.add(grandParent);
                listOfFullTiers.add(parent);
            }

            TierSet setOfDirty = new TierSet(mapOfDirtyTiers);
            for (;;)
            {
                grandParent = parent;
                parent = inner;
                Branch branch = parent.find(criteria);
                Tier tier = parent.getTier(txn, branch.getLeftKey());

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
                            Split split = full.split(txn, criteria, setOfDirty);
                            if (split == null)
                            {
                                tier = full;
                            }
                            else
                            {
                                if (reciever == null)
                                {
                                    reciever = (InnerTier) full;
                                    reciever.splitRootTier(txn, split, setOfDirty);
                                }
                                else
                                {
                                    reciever.replace(full, split, setOfDirty);
                                    if (parent != null)
                                    {
                                        parent.find(criteria).setSize(reciever.getSize());
                                        setOfDirty.add(parent);
                                    }
                                }
                                branch = reciever.find(criteria);
                                tier = reciever.getTier(txn, branch.getLeftKey());
                            }
                        }
                        while (ancestors.hasNext());
                    }

                    LeafTier leaf = (LeafTier) tier;
                    branch.setSize(branch.getSize() + 1);
//                    setOfDirty.add(parent);
                    leaf.insert(txn, criteria, setOfDirty);
                    break;
                }

                inner = (InnerTier) tier;
            }

            size++;
        }

        public Cursor find(Object object)
        {
            return find(structure.getCriterion().newCriteria(txn, object));
        }

        public Cursor find(Comparison comparison, Object object)
        {
            return find(structure.getCriterion().newCriteria(comparison, txn, object));
        }

        public Cursor find(Strata.Criteria criteria)
        {
            InnerTier tier = getRoot();
            for (;;)
            {
                Branch branch = tier.find(criteria);
                if (tier.getChildType() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.getTier(txn, branch.getLeftKey());
                    return leaf.find(txn, criteria);
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
        }

        public Collection remove(Object object)
        {
            Criteria criteria = structure.getCriterion().newCriteria(txn, object);
            TierSet setOfDirty = new TierSet(mapOfDirtyTiers);
            LinkedList listOfAncestors = new LinkedList();
            InnerTier inner = null;
            InnerTier parent = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                listOfAncestors.addLast(tier);
                parent = tier;
                Branch branch = parent.find(criteria);
                if (branch.getObject() != Branch.TERMINAL && criteria.exactMatch(branch.getObject()))
                {
                    inner = tier;
                }
                if (tier.getChildType() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.getTier(txn, branch.getLeftKey());
                    Collection collection = leaf.remove(txn, criteria);
                    branch.setSize(leaf.getSize());
                    setOfDirty.add(parent);

                    if (inner != null)
                    {
                        int objectCount = leaf.getSize();
                        if (objectCount == 0)
                        {
                            int index = parent.getIndexOfTier(leaf);
                            if (inner.getChildType() == Tier.LEAF)
                            {
                                parent.removeLeafTier(leaf, setOfDirty);
                            }
                            else
                            {
                                Branch newPivot = parent.get(index - 1);
                                parent.removeLeafTier(leaf, setOfDirty);
                                parent.replacePivot(structure.getCriterion().newCriteria(txn, newPivot.getObject()), Branch.TERMINAL, setOfDirty);
                                inner.replacePivot(criteria, newPivot.getObject(), setOfDirty);
                            }
                        }
                        else
                        {
                            Object newPivot = leaf.get(objectCount - 1);
                            inner.replacePivot(criteria, newPivot, setOfDirty);
                        }
                    }

                    Tier child = leaf;
                    Iterator ancestors = listOfAncestors.iterator();
                    while (ancestors.hasNext())
                    {
                        InnerTier merge = (InnerTier) ancestors.next();
                        merge.merge(txn, child, setOfDirty);
                    }

                    size -= collection.size();
                    return collection;
                }
                tier = (InnerTier) parent.getTier(txn, branch.getLeftKey());
            }
        }

        public Cursor values()
        {
            Branch branch = null;
            InnerTier tier = getRoot();
            for (;;)
            {
                branch = tier.get(0);
                if (tier.getChildType() == Tier.LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.getTier(txn, branch.getLeftKey());
            }
            return new ForwardCursor(structure, txn, (LeafTier) tier.getTier(txn, branch.getLeftKey()), 0);
        }

        public void write()
        {
            if (mapOfDirtyTiers.size() != 0)
            {
                Iterator tiers = mapOfDirtyTiers.values().iterator();
                while (tiers.hasNext())
                {
                    Tier tier = (Tier) tiers.next();
                    tier.write(txn);
                }
                mapOfDirtyTiers.clear();
            }
        }

        public void revert()
        {
            if (mapOfDirtyTiers.size() != 0)
            {
                Iterator tiers = mapOfDirtyTiers.values().iterator();
                while (tiers.hasNext())
                {
                    Tier tier = (Tier) tiers.next();
                    tier.revert(txn);
                }
                mapOfDirtyTiers.clear();
            }
        }

        public void copacetic()
        {
            getRoot().copacetic(txn, new Copacetic(structure.newComparator(txn)));
            Iterator iterator = values();
            if (getSize() != 0)
            {
                Comparator comparator = structure.newComparator(txn);
                Object previous = iterator.next();
                for (int i = 1; i < size; i++)
                {
                    if (!iterator.hasNext())
                    {
                        throw new IllegalStateException();
                    }
                    Object next = iterator.next();
                    if (comparator.compare(previous, next) > 0)
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

        private InnerTier getRoot()
        {
            return structure.getStorage().getInnerTier(structure, txn, rootKey);
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
                leaf = structure.getStorage().getLeafTier(structure, txn, leaf.getNextLeafKey());
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
                leaf = structure.getStorage().getLeafTier(structure, txn, leaf.getPreviousLeafKey());
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
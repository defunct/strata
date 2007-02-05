package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Doesn't need to handle new types dynamically, does it?
 * <p>
 * The first use case for this project is an index of articles by GUID, from
 * which one can create an atom feed.
 * <p>
 * Only need identities. Only need one type of node.
 * <p>
 * <ul>
 * <li>Will <strong>not</strong> support multiple types of nodes.
 * <li>Will <strong>not</strong> support variable length keys.
 * <li>Will <strong>not</strong> store information in leaves, only pointers.
 * <li>Will <strong>not</strong> cache leaf objects.
 * <li>Will hold <strong>strong</strong> references to full text of key.
 * </ul>
 */
public class Strata
{
    public final static ObjectLoader NULL_OBJECT_LOADER = new ObjectLoader()
    {
        public Object load(Object storage, Object key)
        {
            return key;
        }
    };

    private final Structure structure;

    private final InnerTier root;

    private int size;

    public Strata()
    {
        this(new ArrayListTierServer(), null, new ObjectCriteriaServer());
    }

    public Strata(Storage storage, Object txn, CriteriaServer criterion)
    {
        this.structure = new Structure(storage, criterion, 5);
        this.root = structure.getStorage().newInnerPage(structure, txn, Tier.LEAF);
        this.root.add(new Branch(structure.getStorage().newLeafPage(structure, txn).getKey(), Branch.TERMINAL, 0));
    }

    public static Criteria criteria(Comparable comparable)
    {
        return new ObjectCriteria(comparable);
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
        private CriteriaServer criterion = new ObjectCriteriaServer();

        private Storage storage = new ArrayListTierServer();

        public Strata create(Object txn)
        {
            return new Strata(storage, txn, criterion);
        }

        public void setCriteriaServer(CriteriaServer criterion)
        {
            this.criterion = criterion;
        }

        public void setStorage(Storage storage)
        {
            this.storage = storage;
        }
    }

    public class Structure
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
    }

    public interface ObjectLoader
    {
        public Object load(Object txn, Object key);
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

    public static class ObjectCriteriaServer
    implements CriteriaServer
    {
        public Criteria newCriteria(Object object)
        {
            return new ObjectCriteria(object);
        }
    }

    public static class ObjectCriteria
    implements Criteria
    {
        private final Object criteria;

        public ObjectCriteria(Object criteria)
        {
            this.criteria = criteria;
        }

        public int partialMatch(Object object)
        {
            return ((Comparable) criteria).compareTo(object);
        }

        public boolean exactMatch(Object object)
        {
            return criteria.equals(object);
        }

        public Object getObject()
        {
            return criteria;
        }
    }

    public static class ValuesCriteria
    implements Criteria
    {
        public int partialMatch(Object object)
        {
            throw new UnsupportedOperationException();
        }

        public boolean exactMatch(Object object)
        {
            return true;
        }

        public Object getObject()
        {
            throw new UnsupportedOperationException();
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
            InnerTier parent = null;
            InnerTier inner = root;

            if (inner.isFull())
            {
                listOfFullTiers.add(parent);
            }

            for (;;)
            {
                parent = inner;
                Branch branch = inner.find(criteria);
                Tier tier = parent.load(txn, branch.getKeyOfLeft());

                if (tier.isFull())
                {
                    listOfFullTiers.add(parent);
                }
                else
                {
                    listOfFullTiers.clear();
                }

                if (parent.getTypeOfChildren() == Tier.LEAF)
                {
                    if (tier.isFull())
                    {
                        listOfFullTiers.add(tier);

                        Iterator ancestors = listOfFullTiers.iterator();
                        tier = (Tier) ancestors.next();
                        for (;;)
                        {
                            InnerTier reciever = (InnerTier) tier;
                            Tier full = (Tier) ancestors.next();
                            Split split = full.split(txn, criteria);
                            if (split == null)
                            {
                                tier = full;
                                break;
                            }
                            else if (reciever == null)
                            {
                                reciever = (InnerTier) full;
                                reciever.splitRootTier(txn, split);
                            }
                            else
                            {
                                reciever.replace(full, split);
                            }
                            branch = reciever.find(criteria);
                            tier = reciever.load(txn, branch.getKeyOfLeft());
                            if (!ancestors.hasNext())
                            {
                                break;
                            }
                        }
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

        public Collection find(Object object)
        {
            return find(structure.getCriterion().newCriteria(object));
        }

        public Collection find(Strata.Criteria criteria)
        {
            InnerTier tier = root;
            for (;;)
            {
                Branch branch = tier.find(criteria);
                if (tier.getTypeOfChildren() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.load(txn, branch.getKeyOfLeft());
                    return leaf.find(txn, criteria);
                }
                tier = (InnerTier) tier.load(txn, branch.getKeyOfLeft());
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
                if (tier.getTypeOfChildren() == Tier.LEAF)
                {
                    LeafTier leaf = (LeafTier) tier.load(txn, branch.getKeyOfLeft());
                    Collection collection = leaf.remove(txn, criteria);
                    branch.setSize(branch.getSize() - collection.size());

                    if (inner != null)
                    {
                        int objectCount = leaf.getSize();
                        if (objectCount == 0)
                        {
                            int index = parent.getIndexOfTier(leaf);
                            if (inner.getTypeOfChildren() == Tier.LEAF)
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

                    Tier childTier = leaf;
                    Iterator ancestors = listOfAncestors.iterator();
                    while (ancestors.hasNext())
                    {
                        InnerTier innerTier = (InnerTier) ancestors.next();
                        if (innerTier.canMerge(childTier))
                        {
                            innerTier.merge(txn, childTier);
                        }
                    }

                    size -= collection.size();
                    return collection;
                }
                tier = (InnerTier) parent.load(txn, branch.getKeyOfLeft());
            }
        }

        public Collection values()
        {
            Branch branch = null;
            InnerTier tier = root;
            for (;;)
            {
                branch = tier.get(0);
                if (tier.getTypeOfChildren() == Tier.LEAF)
                {
                    break;
                }
                tier = (InnerTier) tier.load(txn, branch.getKeyOfLeft());
            }
            return new LeafCollection(structure, txn, (LeafTier) tier.load(txn, branch.getKeyOfLeft()), 0, new ValuesCriteria());
        }

        public void copacetic()
        {
            root.copacetic(txn, new Copacetic(new CopaceticComparator()));
            Iterator iterator = values().iterator();
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

    public class CopaceticComparator
    implements Comparator
    {
        public int compare(Object left, Object right)
        {
            return structure.getCriterion().newCriteria(left).partialMatch(right);
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
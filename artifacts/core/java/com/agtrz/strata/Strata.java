package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.agtrz.operators.True;
import com.agtrz.swag.util.ComparatorEquator;
import com.agtrz.swag.util.Equator;

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
    private final Storage storage;

    private final InnerTier root;

    private int size;

    public Strata(Comparator comparator, Equator equator)
    {
        this.storage = new Storage(new ArrayListPager(), null, comparator, equator, 5);
        this.root = storage.getPager().newInnerPage(storage, Tier.LEAF);
    }

    public void copacetic()
    {
        root.copacetic(new Copacetic(storage.getComparator()));
        Iterator iterator = values().iterator();
        if (getSize() != 0)
        {
            Comparator comparator = storage.getComparator();
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

    public Strata(Comparator comparator)
    {
        this(comparator, new ComparatorEquator(comparator));
    }

    public Strata()
    {
        this(new ComparableComparator());
    }

    public int getSize()
    {
        return size;
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
            tier = (InnerTier) tier.load(branch.getKeyOfLeft());
        }
        return new LeafCollection(storage, (LeafTier) tier.load(branch.getKeyOfLeft()), 0, True.INSTANCE);
    }

    public void insert(Object object)
    {
        insert(object, null);
    }

    public void insert(Object object, Object keyOfObject)
    {
        // Maintaining a list of tiers to split.
        //
        // During the decent into the tree, we check for full tiers. When a
        // full tier is first encountered, a list of full tiers is begun
        // starting with the parent of the first full tier, and followed by
        // all of the full tiers to the leaf tier. If less than full tier is
        // encountered, we reset the list of tiers.
        //
        // The list of tiers is locked in order from upper most teirs to the
        // leaf.

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
            Branch branch = inner.find(object);
            Tier tier = parent.load(branch.getKeyOfLeft());

            if (tier.isFull())
            {
                listOfFullTiers.add(parent);
            }
            else
            {
                listOfFullTiers.clear();
            }

            if (tier.isLeaf())
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
                        Split split = full.split(object, keyOfObject);
                        if (split == null)
                        {
                            tier = full;
                            break;
                        }
                        else if (reciever == null)
                        {
                            reciever = (InnerTier) full;
                            reciever.splitRootTier(split);
                        }
                        else
                        {
                            reciever.replace(full, split);
                        }
                        branch = reciever.find(object);
                        tier = reciever.load(branch.getKeyOfLeft());
                        if (!ancestors.hasNext())
                        {
                            break;
                        }
                    }
                }

                LeafTier leaf = (LeafTier) tier;
                branch.setSize(branch.getSize() + 1);
                leaf.insert(object);
                break;
            }

            inner = (InnerTier) tier;
        }

        size++;
    }

    public Collection remove(Object object)
    {
        LinkedList listOfAncestors = new LinkedList();
        InnerTier inner = null;
        InnerTier parent = null;
        InnerTier tier = root;
        Equator equator = storage.getEquator();
        for (;;)
        {
            listOfAncestors.addLast(tier);
            parent = tier;
            Branch branch = tier.find(object);
            if (branch.getObject() != Branch.TERMINAL && equator.equals(branch.getObject(), object))
            {
                inner = tier;
            }
            if (tier.getTypeOfChildren() == Tier.LEAF)
            {
                LeafTier leaf = (LeafTier) tier.load(branch.getKeyOfLeft());
                Collection collection = leaf.remove(object, equator);
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
                            parent.replacePivot(newPivot.getObject(), newPivot.getKeyOfObject(), Branch.TERMINAL);
                            inner.replacePivot(object, newPivot.getObject(), newPivot.getKeyOfObject());
                        }
                    }
                    else
                    {
                        Object newPivot = leaf.get(objectCount - 1);
                        Object keyOfNewPivot = leaf.getKey(objectCount - 1);
                        inner.replacePivot(object, newPivot, keyOfNewPivot);
                    }
                }

                Tier childTier = leaf;
                Iterator ancestors = listOfAncestors.iterator();
                while (ancestors.hasNext())
                {
                    InnerTier innerTier = (InnerTier) ancestors.next();
                    if (innerTier.canMerge(childTier))
                    {
                        innerTier.merge(childTier);
                    }
                }

                size -= collection.size();
                return collection;
            }
            tier = (InnerTier) parent.load(branch.getKeyOfLeft());
        }
    }

    public Collection find(Object object)
    {
        InnerTier tier = root;
        for (;;)
        {
            Branch branch = tier.find(object);
            if (tier.getTypeOfChildren() == Tier.LEAF)
            {
                LeafTier leaf = (LeafTier) tier.load(branch.getKeyOfLeft());
                return leaf.find(object);
            }
            tier = (InnerTier) tier.load(branch.getKeyOfLeft());
        }
    }

    public final static class ComparableComparator
    implements Comparator
    {
        public int compare(Object left, Object right)
        {
            return ((Comparable) left).compareTo(right);
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
package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

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
    private final InnerTier root;

    private final StrataSchema schema;

    private final Comparator comparator;

    private final Equator equator;

    private int size;

    public Strata(Comparator comparator, Equator equator)
    {
        this.root = new InnerTier(comparator, 5);
        this.schema = new StrataSchema();
        this.comparator = comparator;
        this.equator = equator;
    }

    public void copacetic()
    {
        root.copacetic(new Copacetic(comparator));
        Iterator iterator = values().iterator();
        if (size != 0)
        {
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

    public StrataSchema getSchema()
    {
        return schema;
    }

    public Collection values()
    {
        Branch branch = null;
        InnerTier tier = root;
        for (;;)
        {
            branch = tier.getBranch(0);
            if (branch.getLeft().isLeaf())
            {
                break;
            }
            tier = (InnerTier) branch.getLeft();
        }
        return new LeafCollection((LeafTier) branch.getLeft(), 0, True.INSTANCE);
    }

    public void insert(Object object)
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
            Tier tier = branch.getLeft();

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
                        Split split = full.split(object);
                        if (split == null)
                        {
                            tier = full;
                            break;
                        }
                        else if (reciever == null)
                        {
                            reciever = (InnerTier) full;
                            reciever.replace(split);
                        }
                        else
                        {
                            reciever.replace(full, split);
                        }
                        tier = reciever.find(object).getLeft();
                        if (!ancestors.hasNext())
                        {
                            break;
                        }
                    }
                }

                LeafTier leaf = (LeafTier) tier;
                leaf.insert(object);
                break;
            }

            inner = (InnerTier) tier;
        }

        size++;
    }

    public Collection remove(Object object)
    {
        InnerTier inner = null;
        InnerTier parent = null;
        InnerTier tier = root;
        for (;;)
        {
            parent = tier;
            Branch branch = tier.find(object);
            if (equator.equals(branch.getObject(), object))
            {
                inner = tier;
            }
            if (branch.getLeft().isLeaf())
            {
                LeafTier leaf = (LeafTier) branch.getLeft();
                Collection collection = leaf.remove(inner, parent, object, equator);
                size -= collection.size();
                return collection;
            }
        }
    }

    public Collection find(Object object)
    {
        InnerTier tier = root;
        for (;;)
        {
            Branch branch = tier.find(object);
            if (branch.getLeft().isLeaf())
            {
                LeafTier leaf = (LeafTier) branch.getLeft();
                return leaf.find(object);
            }
            tier = (InnerTier) branch.getLeft();
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
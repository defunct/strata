package com.agtrz.strata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
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
    private final InnerTier root;

    private final StrataSchema schema;

    private final Comparator comparator;

    public Strata(Comparator comparitor)
    {
        this.root = new InnerTier(5);
        this.schema = new StrataSchema();
        this.comparator = comparitor;
    }

    public void copacetic()
    {
        root.copacetic(new Copacetic(comparator));
    }

    public Strata()
    {
        this(new ComparableComparator());
    }

    public StrataSchema getSchema()
    {
        return schema;
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
            Branch branch = inner.find(comparator, object);
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
                        Split split = full.split(comparator);
                        if (reciever == null)
                        {
                            reciever = (InnerTier) full;
                            reciever.replace(split);
                        }
                        else
                        {
                            reciever.replace(comparator, full, split);
                        }
                        tier = reciever.find(comparator, object).getLeft();
                        if (!ancestors.hasNext())
                        {
                            break;
                        }
                    }
                }

                LeafTier leaf = (LeafTier) tier;
                leaf.insert(comparator, object);
                break;
            }

            inner = (InnerTier) tier;
        }
    }

    public Collection find(Object object)
    {
        InnerTier tier = root;
        for (;;)
        {
            Branch branch = tier.find(comparator, object);
            if (branch.getLeft().isLeaf())
            {
                LeafTier leaf = (LeafTier) branch.getLeft();
                return leaf.find(comparator, object);
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
        private final Comparator comparator;
        
        private final Set seen;
        
        private Copacetic(Comparator comparator)
        {
            this.comparator = comparator;
            this.seen = new TreeSet(comparator);
        }

        public Comparator getComparator()
        {
            return comparator;
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
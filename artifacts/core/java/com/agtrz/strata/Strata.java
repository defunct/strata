package com.agtrz.strata;

import java.util.Comparator;

/**
 * Doesn't need to handle new types dynamically, does it?
 */
public class Strata
{
    private final TierFactory tiers;
    
    private final StrataSchema schema;
    
    private final Comparator comparator;
    
    public Strata(TierFactory tiers, Comparator comparitor)
    {
        this.tiers = tiers;
        this.schema = new StrataSchema();
        this.comparator = comparitor;
    }
    
    public Strata(TierFactory tiers)
    {
        this(tiers, new ComparableComparator());
    }
    
    public StrataSchema getSchema()
    {
        return schema;
    }

    public void insert(Object object)
    {
        Stratifier stratifier = schema.getStratifier(object.getClass());
        Tier tier = tiers.newTier(1, 8);
        for (;;)
        {
            Branch branch = tier.insert(stratifier, comparator, object);
            if (branch.getLeft().isLeaf())
            {
                branch.getLeft().insert(stratifier, comparator, object);
                break;
            }
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
}

/* vim: set et sw=4 ts=4 ai tw=68: */
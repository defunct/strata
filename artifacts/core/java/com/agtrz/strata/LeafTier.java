package com.agtrz.strata;

import java.util.Comparator;

public interface LeafTier
extends Tier
{
    public void insert(Comparator comparator, Object object);

    public Object find(Comparator comparator, Object object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
package com.agtrz.strata;

import java.util.Comparator;

public interface InnerTier
extends Tier
{
    public void replace(Split split);
    
    public void replace(Comparator comparator, Tier tier, Split split);
    
    public Branch find(Comparator comparator, Object object);
}

/* vim: set et sw=4 ts=4 ai tw=68: */
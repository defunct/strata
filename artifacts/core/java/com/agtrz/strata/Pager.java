/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public interface Pager
{
    public PageLoader getInnerPageLoader();

    public PageLoader getLeafPageLoader();
    
    public InnerTier newInnerPage(Storage storage, short typeOfChildren);
    
    public LeafTier newLeafPage(Storage storage);
    
    public Object getNullKey();
    
    public boolean isKeyNull(Object object);
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
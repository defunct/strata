/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

public class True
implements UrnaryOperator
{
    public final static UrnaryOperator INSTANCE = new True();
    
    private True()
    {
    }
    
    public boolean operate(Object object)
    {
        return true;
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.lang.ref.ReferenceQueue;
import java.util.HashMap;
import java.util.Map;

class BentoStorageBase
{
    protected final transient ReferenceQueue queue = new ReferenceQueue();

    protected final transient Map mapOfTiers = new HashMap();
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
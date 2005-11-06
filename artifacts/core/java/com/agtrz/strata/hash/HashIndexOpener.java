/*
 * Copyright The Engine Room, LLC 2005. All Right Reserved.
 */
package com.agtrz.strata.hash;

import java.net.URI;
import java.nio.ByteBuffer;

import com.agtrz.sheaf.NIO;
import com.agtrz.sheaf.SheafLock;
import com.agtrz.sheaf.SheafLocker;
import com.agtrz.strata.Index;
import com.agtrz.strata.IndexOpener;
import com.agtrz.strata.StrataIOException;
import com.agtrz.swag.danger.Danger;

/**
 * @author Alan Gutierez
 */
final class HashIndexOpener
implements IndexOpener
{
    public Index open(URI uriOfIndex, SheafLocker locker)
    {
        SheafLock shared = locker.newSharedLock();
        try
        {
            int pageOfHeader = locker.getSheaf()
                                     .getSchema()
                                     .getStaticPageNumber(uriOfIndex);
            ByteBuffer bytes = shared.getRead().read(pageOfHeader);
            /* int pageOfHashes = */ bytes.getInt();
            String className = NIO.getString(bytes, bytes.getInt());
            try
            {
                return new HashIndex(getClass().getClassLoader()
                                               .loadClass(className));
            }
            catch (ClassNotFoundException e)
            {
                Danger danger = Danger.raise(HashIndexFactory.class)
                                      .newDanger("class.not.found");
                throw new StrataIOException(danger, e);
            }
        }
        finally
        {
            shared.release();
        }
    }
}


/* vim: set et sw=4 ts=4 ai tw=72: */

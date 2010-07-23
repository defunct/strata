package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public class IntegerTierPool implements Pool<Integer, IntegerTier> {
    public Tier<Integer, IntegerTier> get(Stash stash, IntegerTier address) {
        return address;
    }
}

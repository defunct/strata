package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public class IntegerTierStorage implements Storage<Integer, IntegerTier> {
    public Tier<Integer, IntegerTier> allocate(boolean leaf, int capacity) {
        IntegerTier tier = new IntegerTier();
        tier.setChildLeaf(leaf);
        return tier;
    }
    
    public void free(Stash stash, IntegerTier address) {
    }
    
    public IntegerTier getNull() {
        return null;
    }
    
    public boolean isNull(IntegerTier address) {
        return address == null;
    }
    
    public Tier<Integer, IntegerTier> load(Stash stash, IntegerTier address) {
        return address;
    }
    
    public void write(Stash stash, Tier<Integer, IntegerTier> tier) {
    }
}

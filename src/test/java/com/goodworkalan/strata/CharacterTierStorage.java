package com.goodworkalan.strata;

import com.goodworkalan.stash.Stash;

public class CharacterTierStorage implements Storage<Character, CharacterTier> {
    public Tier<Character, CharacterTier> allocate(boolean leaf, int capacity) {
        CharacterTier tier = new CharacterTier();
        tier.setChildLeaf(leaf);
        return tier;
    }
    
    public void free(Stash stash, CharacterTier address) {
    }
    
    public CharacterTier getNull() {
        return null;
    }
    
    public boolean isNull(CharacterTier address) {
        return address == null;
    }
    
    public Tier<Character, CharacterTier> load(Stash stash, CharacterTier address) {
        return address;
    }
    
    public void write(Stash stash, Tier<Character, CharacterTier> tier) {
    }
}

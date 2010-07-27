package com.goodworkalan.strata;

import org.testng.annotations.Test;
import static org.testng.Assert.*;
import com.goodworkalan.stash.Stash;

/**
 * Unit tests for the {@link ExtractorComparableFactory} class.
 *
 * @author Alan Gutierrez
 */
public class ComparableExtractorTest {
    /** Test comparable construction. */
    @Test
    public void newComparable() {
        assertEquals(new ExtractorComparableFactory<String, String>(new Extractor<String, String>() {
            public String extract(Stash stash, String object) {
                return object;
            }
        }).newComparable(new Stash(), "a").compareTo("a"), 0);
    }
}

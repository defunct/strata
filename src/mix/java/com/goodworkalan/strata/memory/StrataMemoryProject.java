package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Strata Memory.
 *
 * @author Alan Gutierrez
 */
public class StrataMemoryProject implements ProjectModule {
    /**
     * Build the project definition for Strata Memory.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.strata/strata-memory/0.1")
                .depends()
                    .production("com.github.bigeasy.strata/strata/0.1")
                    .development("org.testng/testng-jdk15/5.10")
                    .end()
                .end()
            .end();
    }
}

package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Strata File.
 *
 * @author Alan Gutierrez
 */
public class StrataFileProject implements ProjectModule {
    /**
     * Build the project definition for Strata File.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.strata/strata-file/0.1")
                .depends()
                    .production("com.github.bigeasy.strata/strata/0.1")
                    .development("org.testng/testng-jdk15/5.10")
                    .end()
                .end()
            .end();
    }
}

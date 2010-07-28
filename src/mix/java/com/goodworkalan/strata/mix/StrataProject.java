package com.goodworkalan.strata.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.cookbook.JavaProject;

/**
 * Builds the project definition for Strata.
 *
 * @author Alan Gutierrez
 */
public class StrataProject implements ProjectModule {
    /**
     * Build the project definition for Strata.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.strata/strata/0.1.0.1")
                .depends()
                    .production("com.github.bigeasy.stash/stash/0.1")
                    .production("com.mallardsoft/tuple-partial/0.1.0")
                    .development("org.testng/testng-jdk15/5.10")
                    .development("args4j/args4j/2.0.8")
                    .development("org.mockito/mockito-core/1.6")
                    .end()
                .end()
            .end();
    }
}

---
layout: default
title: Strata
---

# Strata Concerns and Decisions

# Type-safety

I know that the type-safe mechanisms that preserve type information down to the
leaf are an expensive boon-doggle that makes the application too large.

Consider:

    Query query = strata.index("name").query();
    query.find("Alan", "Gutierrez");
    query.find("Alan", 1); // Wrong!
    
That would create a query that would create a query on an index that would
potentially be type unsafe, because I'm going to use comparable. That was the
original implementation.

Now I specify all the types, all the way down to the leaf.

But, what keeps me from doing this? 


    Query query = strata.index("name").query();
    query.find("Gutierrez", "Alan", 1);
    
The first example is caught because the type is wrong. The second example fails
because the columns are wrong.

I'm adding a couple hundred kilobytes of bytecode and dozens of artifacts just
to check one form of data error. That can hardly be worth it.

It is so hard to get rid of all this work, since it is so illuminating.

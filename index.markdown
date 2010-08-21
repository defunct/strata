---
layout: default
title: Strata
---

# Strata Concerns and Decisions

## Memory Mapped I/O

The question becomes, :q

## Atomicity

I can imagine that atomic is done the way you imagined. Use a version number and
bless a version. You can probably use a concurrent hash map for great justice.

## Copy on Write and Read/Write Lock

Currently, read/write lock, but could be copy on write. Traverse the tier if it
is not dirty using a volatile reference. There is a read/write lock, but you
ask for the different locks separately, through the tier. When the page is not
dirty, you get a null lock. When it is dirty you get the read lock. You always
get the write lock. Locking occours as it does now, but if the page is not dirty
you use the null lock, then traverse. Dirty or not, the write lock protects the
tier from competing writes. If the page is not dirty, threads can still traverse
to the underlying tree, reading the old tree. (Hard to imagine right now, how
this works when underlying trees are dirty. Oh, the moment you write, if you do
actually have to write, you replace the tier, pretty sure you'll read the old
tree through volatile.)

## Tier I/O

Probably can use a version that returns the bytes written, ugh, no its a
different UI for each.

Anyway, the tiers are unions, so thought it would be better to just have the
tier sort out the need or un-needed ness of branching.

## Clusters

Thinking about clusters. If the cluster actually references data in the leaf,
and the leaf is a variable length block, like a bunch of pages in a larger file,
then you can reference indexed data in the leaf, by pointing directly to the
leaf in the inner tier. The inner tier can read the data from the leaf, its
value is found in the first record of the leaf, so simply point to the leaf,
then go read the data from the leaf.

An optimization could be to use the log to write to Strata, and have Strata be
the keeper of the data, in clusters. You could, conceivably, maintain the
custers as text files, by using "\n" as empty space. You can also break them up.

If you do have a huge data set, though, that is continguous, it is probably
growing over time, like statuses, or telemetrics, in which case, the write ahead
log would end up being as clustered as anything else.

I suppose, for a small amount of data, the duplication in a clustered index,
with the data "disk cached" (isn't any cache a duplicate) by writing out the
record again in the cluster, or for a small record, would be optimum.

## ByteBuffer

Your new leaf cassette interface, it can be whatever it needs to be. For Memento
primary indicies, it can compare ByteBuffers.

## Fork

Rather than write a defense, or prove how difficult it is by going through with
it, I'm going to fork Strata, and move forward, and just accept that it is
harder to explain the type system to Java, for the sake of correctness, that it
is making the code too big and too difficult understand, and there are still
many complicated features that I want to add, and there is too much complexity
already, and the types are making things inflexible. There is already the
concept of a union, branches and leaves, that requires a runtime cast,
invaraibly, no way around it.

And basically, making this "type-safe", has made this too much an exercise in
the Java type system, and it may end up building something that is not what I
need it to be anyway.

But, really, someone is going to say that I gave it up, not as a compromise to
reduce the complexity, not because I am not smart enough, or rather, I am
stupid.

I'm not sure that there is much more to gain, and I can get great things done
quickly, if I don't ...

Also, you can't put your finger on it, but...

Now that you are building a distributed database, pushing the type safety all
the way down, down through the teirs and into the leaves and out to disk, it is
rather onerous, when... 

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

## Visualization

You're not going to be able to do a visualization, so just forget it. How about
you come up with some metrics, then measure the tree with those metrics and log
it after every operation? Do this until you are reasonably confident in the code.

You can also start adding tests, getting the coverage up.

Is there a visualizaition, though. Grr. It seems like that is the right way to
go, because then you could start to see things. Maybe, you need to rebuild your
tree view, or maybe you just need to build a web application that can visualize
the tree. But, you cannot afford to get into object layouts right yet.

Oh, fuck, why not? Algorithms, right?

Ability to snapshot and move to someplace else for inspection.

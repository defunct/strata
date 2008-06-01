/* Copyright Alan Gutierrez 2006 */
package com.agtrz.strata;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Random;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class StrataStressor
{
    public final static String[] ALPHABET = new String[] { "alpha", "beta", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whisky", "x-ray", "zebra" };

    @Option(name = "-r", usage = "Replay a file.")
    private File replay;

    @Option(name = "-o", usage = "Output file.")
    private File output;

    private final Random random;

    public StrataStressor()
    {
        this.random = new Random();
    }

    private interface Operation
    {
        public void operate(Strata.Query query);
    }

    public final static class Add
    implements Operation, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Compound compound;

        public Add(Compound compound)
        {
            this.compound = compound;
        }

        public void operate(Strata.Query query)
        {
            query.insert(compound);
        }
    }

    public final static class Remove
    implements Operation, Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final Compound compound;

        public Remove(Compound compound)
        {
            this.compound = compound;
        }

        public void operate(Strata.Query query)
        {
            query.remove(compound);
        }
    }

    public final static class Compound
    implements Serializable
    {
        private static final long serialVersionUID = 20070208L;

        private final String one;

        private final String two;

        public Compound(String one, String two)
        {
            this.one = one;
            this.two = two;
        }

        public String getOne()
        {
            return one;
        }

        public String getTwo()
        {
            return two;
        }
    }

    public final static class CompoundComparator<T>
    implements Comparator<T>
    {
        public int compare(T left, T right)
        {
            return ((Compound) left).getOne().compareTo(((Compound) right).getOne());
        }
    }

    public void dump(Strata strata, ObjectOutputStream out) throws IOException
    {
        Strata.Cursor values = strata.query(null).first();
        while (values.hasNext())
        {
            Compound compound = (Compound) values.next();
            out.writeObject(new Add(compound));
        }
    }

    public Compound newCompound()
    {
        String one = ALPHABET[random.nextInt(ALPHABET.length)];
        String two = ALPHABET[random.nextInt(ALPHABET.length)];
        return new Compound(one, two);
    }

    public void kickTires(int count, int max, ObjectOutputStream out) throws IOException
    {
        int size = 0;
        Strata strata = new Strata();
        Strata.Query query = strata.query(null);
        for (int i = 0; i < count; i++)
        {
            Operation operation = null;
            if (size == 0)
            {
                operation = new Add(newCompound());
            }
            else
            {
                double probablity = ((double) size) / max;
                int add = random.nextInt((int) (probablity * max));
                if (add < (max / 2))
                {
                    operation = new Add(newCompound());
                    size++;
                }
                else
                {
                    Compound compound = null;
                    Strata.Cursor collection = null;
                    do
                    {
                        compound = newCompound();
                        collection = query.find(compound);
                    }
                    while (!collection.hasNext());

                    operation = new Remove(compound);
                    size--;
                }
            }
            out.writeObject(operation);
            operation.operate(query);
            query.copacetic();
        }
    } 
    
    public static void main(String[] args)
    {
        StrataStressor stressor = new StrataStressor();
        CmdLineParser parser = new CmdLineParser(stressor);
        try
        {
            parser.parseArgument(args);
        }
        catch (CmdLineException e)
        {
            System.err.println(e.getMessage());
            System.err.println("java -jar myprogram.jar [options...] arguments...");
            parser.printUsage(System.err);
            return;
        }
        stressor.execute();
    }
    
    public void execute()
    {
        File file = null;
        if ((file = replay) != null)
        {
            ObjectInputStream in = null;
            try
            {
                in = new ObjectInputStream(new FileInputStream(file));
            }
            catch (FileNotFoundException e)
            {
                System.out.print("Cannot find file: " + file.toString());
                System.exit(1);
            }
            catch (IOException e)
            {
                System.out.print("Cannot open file: " + file.toString() + ", " + e.getMessage());
                System.exit(1);
            }
            Strata strata = new Strata();
            Strata.Query query = strata.query(null);
            for (;;)
            {
                Operation operation = null;
                try
                {
                    operation = (Operation) in.readObject();
                }
                catch (EOFException e)
                {
                    break;
                }
                catch (IOException e)
                {
                    System.out.print("Cannot find file: " + file.toString());
                    System.exit(1);
                }
                catch (ClassNotFoundException e)
                {
                    System.out.print("Cannot open file: " + file.toString() + ", " + e.getMessage());
                    System.exit(1);
                }
                if (operation == null)
                {
                    break;
                }
                operation.operate(query);
            }
        }
        else
        {
            file = output;

            if (file == null)
            {
                System.exit(1);
            }

            for (;;)
            {
                ObjectOutputStream out = null;
                try
                {
                    out = new ObjectOutputStream(new FileOutputStream(file));
                }
                catch (IOException e)
                {
                    System.out.print("Cannot open file: " + file.toString() + ", :" + e.getMessage());
                    System.exit(1);
                }
                try
                {
                    new StrataStressor().kickTires(10000, 1000, out);
                }
                catch (RuntimeException e)
                {
                    e.printStackTrace();
                    break;
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    break;
                }
                finally
                {
                    try
                    {
                        out.close();
                    }
                    catch (IOException e)
                    {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */
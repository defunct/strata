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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.agtrz.swag.shell.ArgumentException;
import com.agtrz.swag.shell.CommandLine;
import com.agtrz.swag.shell.InputFileArgument;
import com.agtrz.swag.shell.OutputFileArgument;
import com.agtrz.swag.util.Equator;

public class StrataStressor
{
    private final static String[] ALPHABET = new String[] { "alpha", "beta", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whisky", "x-ray", "zebra" };

    private final Random random;

    public StrataStressor()
    {
        this.random = new Random();
    }

    private interface Operation
    {
        public void operate(Strata strata);
    }

    public final static class Add
    implements Operation, Serializable
    {
        private final Compound compound;

        public Add(Compound compound)
        {
            this.compound = compound;
        }

        public void operate(Strata strata)
        {
            strata.insert(compound);
        }
    }

    public final static class Remove
    implements Operation, Serializable
    {
        private final Compound compound;

        public Remove(Compound compound)
        {
            this.compound = compound;
        }

        public void operate(Strata strata)
        {
            strata.remove(compound);
        }
    }

    private final static class Compound
    implements Serializable
    {
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

    private final static class CompoundComparator
    implements Comparator
    {
        public int compare(Object left, Object right)
        {
            return ((Compound) left).getOne().compareTo(((Compound) right).getOne());
        }
    }

    private final static class CompoundEquator
    implements Equator
    {
        public boolean equals(Object left, Object right)
        {
            return ((Compound) left).getOne().equals(((Compound) right).getOne()) && ((Compound) left).getTwo().equals(((Compound) right).getTwo());
        }
    }

    public void dump(Strata strata, ObjectOutputStream out) throws IOException
    {
        Iterator values = strata.values().iterator();
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
        Strata strata = new Strata(new CompoundComparator(), new CompoundEquator());
        for (int i = 0; i < count; i++)
        {
            Operation operation = null;
            if (strata.size() == 0)
            {
                operation = new Add(newCompound());
            }
            else
            {
                double probablity = ((double) strata.size())/ max;
                int add = random.nextInt((int) (probablity * max));
                if (add < (max / 2))
                {
                    operation = new Add(newCompound());
                }
                else
                {
                    Compound compound = null;
                    Collection collection = null;
                    do
                    {
                        compound = newCompound();
                        collection = strata.find(compound);
                    }
                    while (!collection.iterator().hasNext());

                    operation = new Remove(compound);
                }
            }
            out.writeObject(operation);
            operation.operate(strata);
            strata.copacetic();
        }
    }

    public static void main(String[] args)
    {
        List listOfArguments = new ArrayList();
        listOfArguments.add(new InputFileArgument("replay"));
        listOfArguments.add(new OutputFileArgument("output", true));
        CommandLine commandLine = null;
        try
        {
            commandLine = new CommandLine(args, listOfArguments);
        }
        catch (ArgumentException e)
        {
            System.out.println(e.getMessage());
            System.exit(1);
        }
        File file = null;
        if ((file = (File) commandLine.getArgumentValue("replay")) != null)
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
            Strata strata = new Strata(new CompoundComparator(), new CompoundEquator());
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
                operation.operate(strata);
            }
        }
        else
        {
            file = (File) commandLine.getArgumentValue("output");
            
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
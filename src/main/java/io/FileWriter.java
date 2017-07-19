package io;

import java.io.BufferedWriter;
import java.io.IOException;

import util.Util;

public class FileWriter
{
    private BufferedWriter writer;
    private int count;

    public FileWriter( String pathAndFileName )
    {
        writer = Util.getBufferedWriter( pathAndFileName );
    }

    public void write( String str ) throws IOException
    {
        writer.write( str );
        writer.newLine();
        count++;
    }

    public void close()
    {
        System.out.println( count + " records written" );
        Util.close( writer );
    }

}

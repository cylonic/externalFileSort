package io;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import util.Util;

public class FileReader
{
    private BufferedReader reader;
    private int count;

    public FileReader( String pathAndFileName )
    {
        try
        {
            reader = Util.getBufferedReader( pathAndFileName );
        } catch ( UnsupportedEncodingException | FileNotFoundException e )
        {
            throw new RuntimeException( "Couldnt open file: " + pathAndFileName, e );
        }
    }

    public String getNextLine() throws IOException
    {
        try
        {
            String line;
            if ( ( line = reader.readLine() ) != null )
            {
                count++;
                return line;
            } else
            {
                return null;
            }
        } catch ( IOException e )
        {
            throw new IOException( "Couldnt read line " + count );
        }
    }

    public void close()
    {
        System.out.println( count + " records read" );
        Util.close( reader );
    }

}

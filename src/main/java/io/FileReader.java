package io;

import java.io.BufferedReader;
import java.io.IOException;

import util.Util;

public class FileReader
{
    private BufferedReader reader;
    private int count;

    public FileReader( String pathAndFileName )
    {
        reader = Util.getBufferedReader( pathAndFileName );
    }

    public String getNextString() throws IOException
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
        Util.closeQuietly( reader );
    }

}

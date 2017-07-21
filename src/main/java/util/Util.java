package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Util
{

    public static BufferedReader getBufferedReader( String fullFilePath )
    {
        File file = new File( fullFilePath );

        if ( !file.exists() )
        {
            throw new RuntimeException( "File does not exist: " + fullFilePath );
        }

        try
        {
            return Files.newBufferedReader( Paths.get( fullFilePath ), Charset.forName( "UTF-8" ) );
        } catch ( IOException e )
        {
            throw new RuntimeException( "Couldnt open file: " + fullFilePath, e );
        }

    }

    public static BufferedWriter getBufferedWriter( String fullFilePath )
    {
        try
        {
            return Files.newBufferedWriter( Paths.get( fullFilePath ), Charset.forName( "UTF-8" ),
                    StandardOpenOption.TRUNCATE_EXISTING );
        } catch ( IOException e )
        {
            throw new RuntimeException( "Failed to open file: " + fullFilePath );
        }
    }

    public static void closeQuietly( Object toClose )
    {
        if ( null != toClose )
        {
            if ( toClose instanceof Reader )
            {
                Reader r = (Reader) toClose;
                try
                {
                    r.close();
                } catch ( IOException e )
                {
                    // Quiet
                }
            } else if ( toClose instanceof Writer )
            {
                Writer w = (Writer) toClose;
                try
                {
                    w.close();
                } catch ( IOException e )
                {
                    // Quiet
                }

            }

        }
    }

}

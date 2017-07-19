package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;

public class Util
{

    public static BufferedReader getBufferedReader( String fullFilePath )
            throws UnsupportedEncodingException, FileNotFoundException
    {
        File file = new File( fullFilePath );

        if ( !file.exists() )
        {
            throw new RuntimeException( "File does not exist: " + fullFilePath );
        }

        return new BufferedReader( new InputStreamReader( new FileInputStream( fullFilePath ), "UTF-8" ) );
    }

    public static BufferedWriter getBufferedWriter( String fullFilePath )
    {
        File file = new File( fullFilePath );

        try
        {
            return new BufferedWriter( new OutputStreamWriter( new FileOutputStream( file ), "UTF-8" ) );
        } catch ( UnsupportedEncodingException | FileNotFoundException e )
        {
            throw new RuntimeException( "Failed to open file: " + fullFilePath );
        }
    }

    public static void close( Object toClose )
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
                    e.printStackTrace();
                }
            } else if ( toClose instanceof Writer )
            {
                Writer w = (Writer) toClose;
                try
                {
                    w.close();
                } catch ( IOException e )
                {
                    e.printStackTrace();
                }

            }

        }
    }

}

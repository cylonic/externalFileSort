package util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import factory.DoubleItemFactory;
import factory.IntegerItemFactory;
import factory.ItemFactory;
import factory.StringItemFactory;
import io.FileWriter;
import model.Item.ItemType;
import processor.Processor;

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
                    StandardOpenOption.CREATE_NEW );
        } catch ( IOException e )
        {
            throw new RuntimeException( "Failed to open file: " + fullFilePath );
        }
    }

    public static ItemFactory getFactory( ItemType type )
    {
        if ( type == ItemType.STRING )
        {
            return new StringItemFactory();
        } else if ( type == ItemType.INTEGER )
        {
            return new IntegerItemFactory();
        } else if ( type == ItemType.DOUBLE )
        {
            return new DoubleItemFactory();
        }
        return null;
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

    public static String stripFileExt( String file )
    {
        return file.substring( 0, file.lastIndexOf( "." ) );
    }

    public static String[] splitPathAndFilename( String pathAndName )
    {
        String path = pathAndName.substring( 0, pathAndName.lastIndexOf( "/" ) );
        String name = pathAndName.substring( pathAndName.lastIndexOf( "/" ) );

        return new String[]
        { path, name };
    }

    public static void createDir( String path )
    {
        Path fp = Paths.get( path );

        if ( !Files.exists( fp ) )
        {
            try
            {
                Files.createDirectory( fp );
            } catch ( IOException e )
            {

            }
        }
    }

    public static String createNewBaseFile( String oldBase, String oldName )
    {
        return new StringBuilder().append( oldBase ).append( Util.stripFileExt( oldName ) ).append( "_" ).toString();
    }

    public static void generateFile()
    {
        // List<Item<?>> list = new ArrayList<>();
        FileWriter fw = new FileWriter( "/data/fluffy.txt" );

        for (int i = 0; i < 100_000_000; i++)
        {
            // list.add( new Item<>( ThreadLocalRandom.current().nextInt(
            // 1_000_000 ), ItemType.INTEGER ) );

            if ( i % 1_000_000 == 0 )
            {
                System.out.println( "Written " + i + " records" );
            }

            try
            {
                fw.write( String.valueOf( ThreadLocalRandom.current().nextInt( 1_000_000 ) ) );
            } catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        // list = Sort.sortList( list, true );

        // for (Item<?> line : list)
        // {
        // try
        // {
        // fw.write( line.getItem().toString() );
        // } catch ( IOException e )
        // {
        // e.printStackTrace();
        // }
        // }

        fw.close();
    }

    public static long getFileCount( String dir )
    {

        try
        {
            return Files.list( Paths.get( dir ) ).count();
        } catch ( IOException e )
        {
            e.printStackTrace();
        }
        return 0;
    }

    public static Properties getDefaultProps()
    {
        Properties props = new Properties();
        InputStream is = null;

        try
        {
            is = Processor.class.getClassLoader().getResourceAsStream( "props/default.properties" );
            props.load( is );
        } catch ( IOException e )
        {
            String msg = "Failed to find default.properties file";
            throw new RuntimeException( msg, e );
        } finally
        {
            if ( is != null )
            {
                try
                {
                    is.close();
                } catch ( IOException e )
                {
                    // quiet
                }
            }
        }

        return props;
    }

    public static void main( String[] args )
    {
        // generateFile();
        System.out.println( getFileCount( "/data/shards/" ) );
    }

}

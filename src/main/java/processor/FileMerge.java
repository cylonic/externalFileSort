package processor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Queue;

import datasource.Datasource;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Util;

public class FileMerge implements Runnable
{
    private CloseableQueue<Item<?>> queue;
    private final ItemType type;

    public FileMerge( final CloseableQueue<Item<?>> q, final ItemType type )
    {
        this.queue = q;
        this.type = type;
    }

    @Override
    public void run()
    {
        mergeSortedFiles();
    }

    public void mergeSortedFiles()
    {

        Datasource<?> leftDs = null;
        Datasource<?> rightDs = null;

        Queue<Path> filesToMerge = Util.prefixedFiles( "/data/shards/", "*" );

        while ( filesToMerge.size() > 1 )
        {
            Path p1 = null;
            Path p2 = null;
            try
            {
                p1 = filesToMerge.poll();
                p2 = filesToMerge.poll();

                leftDs = new Datasource<>( p1.toString(), type );
                rightDs = new Datasource<>( p2.toString(), type );

                mergeFiles( leftDs, rightDs );

            } catch ( UnsupportedEncodingException e )
            {
                throw new RuntimeException( "Improper encoding", e );
            } catch ( IOException e )
            {
                throw new RuntimeException( "Couldnt open file ", e );
            } finally
            {
                if ( leftDs != null )
                {
                    leftDs.close();
                }
                if ( rightDs != null )
                {
                    rightDs.close();
                }

                // filesToMerge = Util.prefixedFiles( "/data/shards/", "*" );
                // queue.close();
            }
        }

        queue.close();

        return;
    }

    private void mergeFiles( Datasource<?> d1, Datasource<?> d2 ) throws IOException
    {
        Item<?> left = d1.getNextItem();
        Item<?> right = d2.getNextItem();

        labelParent: while ( true )
        {

            if ( left == null && right == null )
            {
                break;
            }

            if ( left == null )
            {
                queue.put( right );
                runFileOut( d2 );
                return;
            }

            if ( right == null )
            {
                queue.put( left );
                runFileOut( d1 );
                return;
            }

            while ( left.compareTo( right ) > 0 )
            {
                queue.put( right );
                Item<?> tempRight = d2.getNextItem();

                if ( tempRight == null ) // reader 2 file is empty
                {
                    queue.put( left );
                    runFileOut( d1 );
                    return; // both files should be empty at this point
                }

                if ( left.compareTo( tempRight ) < 0 )
                {
                    queue.put( left );
                    left = d1.getNextItem();
                    right = tempRight;
                    continue labelParent;
                }
                right = tempRight;

            }

            while ( right.compareTo( left ) > 0 )
            {
                queue.put( left );
                Item<?> tempLeft = d1.getNextItem();

                if ( tempLeft == null ) // reader 1 file is empty
                {
                    queue.put( right );
                    runFileOut( d2 );
                    return; // both files should be empty at this point
                }

                if ( right.compareTo( left ) < 0 )
                {
                    queue.put( right );
                    right = d2.getNextItem();
                    left = tempLeft;
                    continue labelParent;
                }
                left = tempLeft;

            }

            if ( left.compareTo( right ) == 0 )
            {
                queue.put( left );
                queue.put( right );
                left = d1.getNextItem();
                right = d2.getNextItem();
            }

        }

    }

    private void runFileOut( Datasource<?> d ) throws IOException
    {
        Item<?> item;
        while ( ( item = d.getNextItem() ) != null )
        {
            queue.put( item );
        }
    }

}

package processor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import datasource.Datasource;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;

public class QueueBuilder implements Runnable
{
    private CloseableQueue queue;
    private final ItemType type;

    public QueueBuilder( final CloseableQueue q, final ItemType type )
    {
        this.queue = q;
        this.type = type;
    }

    @Override
    public void run()
    {
        buildQueue();
    }

    public void buildQueue()
    {

        Datasource<?> leftDs = null;
        Datasource<?> rightDs = null;

        try
        {
            leftDs = new Datasource<>( "/data/fluffy.txt", type );
            rightDs = new Datasource<>( "/data/fluffy2.txt", type );

            Item<?> left = leftDs.getNextItem();
            Item<?> right = rightDs.getNextItem();

            labelParent: while ( true )
            {

                if ( left == null && right == null )
                {
                    break;
                }

                if ( left == null )
                {
                    queue.put( right );
                    runFileOut( rightDs );
                    return;
                }

                if ( right == null )
                {
                    queue.put( left );
                    runFileOut( leftDs );
                    return;
                }

                while ( left.compareTo( right ) > 0 )
                {
                    queue.put( right );
                    Item<?> tempRight = rightDs.getNextItem();

                    if ( tempRight == null ) // reader 2 file is empty
                    {
                        queue.put( left );
                        runFileOut( leftDs );
                        return; // both files should be empty at this point
                    }

                    if ( left.compareTo( tempRight ) < 0 )
                    {
                        queue.put( left );
                        left = leftDs.getNextItem();
                        right = tempRight;
                        continue labelParent;
                    }
                    right = tempRight;

                }

                while ( right.compareTo( left ) > 0 )
                {
                    queue.put( left );
                    Item<?> tempLeft = leftDs.getNextItem();

                    if ( tempLeft == null ) // reader 1 file is empty
                    {
                        queue.put( right );
                        runFileOut( rightDs );
                        return; // both files should be empty at this point
                    }

                    if ( right.compareTo( left ) < 0 )
                    {
                        queue.put( right );
                        right = rightDs.getNextItem();
                        left = tempLeft;
                        continue labelParent;
                    }
                    left = tempLeft;

                }

                if ( left.compareTo( right ) == 0 )
                {
                    queue.put( left );
                    queue.put( right );
                    left = leftDs.getNextItem();
                    right = rightDs.getNextItem();
                }

            }

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
            queue.close();
        }

        return;
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

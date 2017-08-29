package merge;

import java.io.IOException;

import datasource.Datasource;
import model.CloseableQueue;
import model.Item;

public class AscendingMerge extends Merge
{

    public AscendingMerge( CloseableQueue<Item<?>> queue )
    {
        super( queue );
    }

    @Override
    public void merge( Datasource<?> d1, Datasource<?> d2 ) throws IOException
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

}

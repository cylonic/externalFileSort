package merge;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import datasource.Datasource;
import datasource.InputDatasource;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;

public class FileMerge implements Runnable
{
    private CloseableQueue<Item<?>> queue;
    private final ItemType type;
    private final String file1;
    private final String file2;
    private final Merge merge;

    public FileMerge( final String file1, final String file2, final CloseableQueue<Item<?>> q, final ItemType type,
            final boolean ascending )
    {
        this.queue = q;
        this.type = type;
        this.file1 = file1;
        this.file2 = file2;
        this.merge = getMerge( ascending, q );

    }

    @Override
    public void run()
    {
        mergeSortedFiles();
    }

    public void mergeSortedFiles()
    {

        Datasource leftDs = null;
        Datasource rightDs = null;

        try
        {

            leftDs = new InputDatasource<>( file1, type );
            rightDs = new InputDatasource<>( file2, type );

            merge.merge( leftDs, rightDs );

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

    private static Merge getMerge( boolean ascending, CloseableQueue<Item<?>> q )
    {
        if ( ascending )
        {
            return new AscendingMerge( q );
        }

        return new DescendingMerge( q );

    }

}

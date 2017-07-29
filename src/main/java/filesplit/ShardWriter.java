package filesplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.FileWriter;
import model.CloseableListQueue;
import model.Item;
import sorting.Sort;

public class ShardWriter implements Runnable
{
    private String baseFileName;
    private final boolean ascending;
    private CloseableListQueue q;

    public ShardWriter( boolean ascending, String baseFileName, CloseableListQueue q )
    {
        this.baseFileName = baseFileName;
        this.ascending = ascending;
        this.q = q;
    }

    private void work()
    {
        ExecutorService writers = Executors.newFixedThreadPool( 8 );
        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger count = new AtomicInteger( 1 );
        try
        {
            List<Item<?>> items;
            while ( !( q.isClosed() && q.isEmpty() ) )
            {
                items = q.poll( 2, TimeUnit.SECONDS );

                if ( items == null )
                {
                    continue;
                }

                futures.add( writers.submit( getWriterThread( items,
                        ( baseFileName + ( count.getAndIncrement() ) + ".txt" ), ascending ) ) );

            }

        } catch ( InterruptedException e )
        {
            e.printStackTrace();
        } finally
        {
            writers.shutdown();
        }

    }

    private Runnable getWriterThread( List<Item<?>> shard, String filename, boolean ascending )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                FileWriter w = new FileWriter( filename );
                try
                {
                    for (Item<?> item : Sort.sortList( shard, ascending ))
                    {
                        w.write( item.getItem().toString() );
                    }
                } catch ( IOException e )
                {
                    e.printStackTrace();
                } finally
                {
                    w.close();
                }

            }
        };

    }

    @Override
    public void run()
    {
        work();
        System.out.println( "shardWriter done" );
    }

}

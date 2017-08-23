package filesplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.FileWriter;
import model.CloseableQueue;
import model.Item;
import sorting.Sort;
import util.Constants;

public class ShardWriter implements Runnable
{
    private String baseFileName;
    private final boolean ascending;
    private CloseableQueue<List<Item<?>>> q;
    private Properties properties;

    public ShardWriter( boolean ascending, String baseFileName, CloseableQueue<List<Item<?>>> q, Properties props )
    {
        this.baseFileName = baseFileName;
        this.ascending = ascending;
        this.q = q;
        this.properties = props;
    }

    private void work()
    {
        ExecutorService writers = Executors
                .newFixedThreadPool( Integer.parseInt( properties.getProperty( Constants.SHARD_THREAD_COUNT ) ) );
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
    }

}

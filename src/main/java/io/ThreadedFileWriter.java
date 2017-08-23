package io;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import model.CloseableQueue;
import model.Item;

public class ThreadedFileWriter implements Runnable
{
    private CloseableQueue<Item<?>> q;
    private FileWriter writer;

    public ThreadedFileWriter( final CloseableQueue<Item<?>> queue, final String pathAndFileName )
    {
        this.q = queue;
        this.writer = new FileWriter( pathAndFileName );
    }

    @Override
    public void run()
    {
        try
        {
            Item<?> item;
            while ( !( q.isClosed() && q.isEmpty() ) )
            {
                item = q.poll( 2, TimeUnit.SECONDS );

                if ( null == item )
                {
                    continue;
                }
                writer.write( item.getItem().toString() );
            }
        } catch ( IOException e )
        {
            throw new RuntimeException( "Error writing to file" );
        } catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Thread interrupted", e );
        } finally
        {
            writer.close();
        }
    }

}

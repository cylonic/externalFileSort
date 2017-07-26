package model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CloseableQueue
{
    private volatile boolean closed = false;
    private BlockingQueue<Item<?>> q = new LinkedBlockingQueue<>( 100 );

    public void put( Item<?> s )
    {
        if ( isClosed() )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Cant add to already closed queue." );
        }

        try
        {
            q.put( s );
        } catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Thread interrupted" );
        }
    }

    public Item<?> poll( int time, TimeUnit units ) throws InterruptedException
    {
        return q.poll( time, units );
    }

    public Item<?> poll() throws InterruptedException
    {
        return q.take();
    }

    public int size()
    {
        return q.size();
    }

    public boolean isEmpty()
    {
        return q.isEmpty();
    }

    public synchronized boolean isClosed()
    {
        return closed;
    }

    public synchronized void close()
    {
        closed = true;
    }

}

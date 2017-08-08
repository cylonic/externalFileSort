package model;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CloseableQueue<E>
{
    private volatile boolean closed = false;
    private BlockingQueue<E> q;

    public CloseableQueue( int capacity )
    {
        q = new LinkedBlockingQueue<>( capacity );
    }

    public void put( E s )
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

    public E poll( int time, TimeUnit units ) throws InterruptedException
    {
        return q.poll( time, units );
    }

    public E poll() throws InterruptedException
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

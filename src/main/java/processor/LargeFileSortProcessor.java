package processor;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.CloseableQueue;

public class LargeFileSortProcessor
{

    private CloseableQueue queue = new CloseableQueue();

    public static void main( String[] args )
    {
        LargeFileSortProcessor pro = new LargeFileSortProcessor();
        pro.startThreads();

        System.out.println( "Done." );
    }

    public void startThreads()
    {
        ExecutorService readerService = Executors.newSingleThreadExecutor();
        ExecutorService writerService = Executors.newSingleThreadExecutor();

        QueueBuilder readerAndWorker = new QueueBuilder( queue );
        ThreadedFileWriter writer = new ThreadedFileWriter( queue, "/data/output.txt" );

        Future<?> readerFuture = readerService.submit( readerAndWorker );
        Future<?> writerFuture = writerService.submit( writer );

        try
        {
            readerFuture.get();
        } catch ( InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( "Thread interrupted", e );
        } finally
        {
            readerService.shutdown();
        }

        try
        {
            writerFuture.get();
        } catch ( InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( "Thread interrupted", e );
        } finally
        {
            writerService.shutdown();
        }

    }

}

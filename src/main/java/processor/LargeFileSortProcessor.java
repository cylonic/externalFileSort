package processor;

import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.ThreadedFileWriter;
import merge.FileMerge;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Constants;
import util.Util;

public class LargeFileSortProcessor implements Callable<Integer>
{

    private CloseableQueue<Item<?>> queue = new CloseableQueue<>( 15 );
    private Properties properties;
    private String shard1;
    private String shard2;
    private String mergedPathAndFileName;

    public LargeFileSortProcessor( Properties props, String shard1, String shard2, String mergedPathAndFileName )
    {
        this.properties = props;
        this.shard1 = shard1;
        this.shard2 = shard2;
        this.mergedPathAndFileName = mergedPathAndFileName;

    }

    public void startThreads()
    {
        ExecutorService readerService = Executors.newSingleThreadExecutor();
        ExecutorService writerService = Executors.newSingleThreadExecutor();

        ItemType type = ItemType.valueOf( properties.getProperty( Constants.ITEM_TYPE ) );

        FileMerge readerAndWorker = new FileMerge( shard1, shard2, queue, type );
        ThreadedFileWriter writer = new ThreadedFileWriter( queue, mergedPathAndFileName );

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

        Util.deleteFile( shard1 );
        Util.deleteFile( shard2 );

    }

    @Override
    public Integer call() throws Exception
    {
        startThreads();
        return 1;
    }

}

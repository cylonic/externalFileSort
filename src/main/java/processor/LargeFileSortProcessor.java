package processor;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Constants;
import util.Util;

public class LargeFileSortProcessor
{

    private CloseableQueue<Item<?>> queue = new CloseableQueue<>( 15 );
    private Properties properties;
    private String shard1;
    private String shard2;
    private String mergedPathAndFileName;

    public static void main( String[] args )
    {
        Properties props = Util.getDefaultProps();
        LargeFileSortProcessor pro = new LargeFileSortProcessor( props, "/data/shards/fluffy_1.txt",
                "/data/shards/fluffy_1.txt", "/data/shards/output1.txt" );
        pro.startThreads();

        System.out.println( "Done." );
    }

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
            System.out.println( "Reader thread finished." );
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
            System.out.println( "Writer thread finished." );
        } catch ( InterruptedException | ExecutionException e )
        {
            throw new RuntimeException( "Thread interrupted", e );
        } finally
        {
            writerService.shutdown();
        }

    }

}

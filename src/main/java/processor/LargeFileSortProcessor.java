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

public class LargeFileSortProcessor
{

    private CloseableQueue<Item<?>> queue = new CloseableQueue<>( 15 );
    private Properties properties;

    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put( Constants.MERGED_OUTPUT_FILE, "/data/output.txt" );
        props.put( Constants.ITEM_TYPE, "INTEGER" );
        LargeFileSortProcessor pro = new LargeFileSortProcessor( props );
        pro.startThreads();

        System.out.println( "Done." );
    }

    public LargeFileSortProcessor( Properties props )
    {
        this.properties = props;

    }

    public void startThreads()
    {
        ExecutorService readerService = Executors.newSingleThreadExecutor();
        ExecutorService writerService = Executors.newSingleThreadExecutor();

        FileMerge readerAndWorker = new FileMerge( queue,
                ItemType.valueOf( properties.getProperty( Constants.ITEM_TYPE ) ) );
        ThreadedFileWriter writer = new ThreadedFileWriter( queue,
                properties.getProperty( Constants.MERGED_OUTPUT_FILE ) );

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

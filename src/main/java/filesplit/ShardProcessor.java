package filesplit;

import java.util.List;
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

public class ShardProcessor
{
    private Properties properties;

    public static void main( String[] args )
    {
        Properties props = new Properties();
        props.put( Constants.SHARD_INPUT_FILE, "/data/fluffy.txt" );
        props.put( Constants.SHARD_SIZE, "1000000" );
        props.put( Constants.SHARD_THREAD_COUNT, "8" );
        props.put( Constants.QUEUE_SIZE, "15" );
        props.put( Constants.ITEM_TYPE, "INTEGER" );
        props.put( Constants.ASCENDING, "true" );

        ShardProcessor pro = new ShardProcessor( props );
        pro.start();
    }

    public ShardProcessor( Properties props )
    {
        this.properties = props;
    }

    private void start()
    {
        String file = "/data/fluffy.txt";

        CloseableQueue<List<Item<?>>> q = new CloseableQueue<>(
                Integer.parseInt( properties.getProperty( Constants.QUEUE_SIZE ) ) );
        ShardFile sf = new ShardFile( ItemType.valueOf( properties.getProperty( Constants.ITEM_TYPE ) ),
                properties.getProperty( Constants.SHARD_INPUT_FILE ), q, properties );
        String newBase = prepare( file );

        ShardWriter sw = new ShardWriter( Boolean.valueOf( properties.getProperty( Constants.ASCENDING ) ), newBase, q,
                properties );

        ExecutorService readerThread = Executors.newSingleThreadExecutor();
        ExecutorService writerThread = Executors.newSingleThreadExecutor();

        Future<?> rf = readerThread.submit( sf );
        Future<?> wf = writerThread.submit( sw );

        try
        {
            rf.get();
        } catch ( InterruptedException | ExecutionException e )
        {
            e.printStackTrace();
        }

        try
        {
            wf.get();
        } catch ( InterruptedException | ExecutionException e )
        {
            e.printStackTrace();
        }

        readerThread.shutdown();
        writerThread.shutdown();

        System.out.println( "DONE" );

    }

    private String prepare( String file )
    {
        String[] parts = Util.splitPathAndFilename( file );
        String baseName = parts[0] + "/shards";
        Util.createDir( baseName );
        String newBase = Util.createNewBaseFile( baseName, parts[1] );

        return newBase;
    }

}

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
        Properties props = Util.getDefaultProps();

        ShardProcessor pro = new ShardProcessor( props );
        pro.start();
    }

    public ShardProcessor( Properties props )
    {
        this.properties = props;
    }

    private void start()
    {
        String file = properties.getProperty( Constants.SHARD_INPUT_FILE );
        ItemType type = ItemType.valueOf( properties.getProperty( Constants.ITEM_TYPE ) );
        int queueSize = Integer.parseInt( properties.getProperty( Constants.QUEUE_SIZE ) );
        boolean ascending = Boolean.valueOf( properties.getProperty( Constants.ASCENDING ) );

        CloseableQueue<List<Item<?>>> q = new CloseableQueue<>( queueSize );
        ShardFile sf = new ShardFile( type, file, q, properties );
        String newBase = prepare( file );

        ShardWriter sw = new ShardWriter( ascending, newBase, q, properties );

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

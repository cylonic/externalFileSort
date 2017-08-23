package processor;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import filesplit.ShardFile;
import filesplit.ShardWriter;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Constants;
import util.Util;

public class ShardProcessor
{
    private Properties properties;

    public ShardProcessor( Properties props )
    {
        this.properties = props;
    }

    public void start()
    {
        String file = properties.getProperty( Constants.SHARD_INPUT_FILE );
        String outputDir = properties.getProperty( Constants.SHARD_OUTPUT_DIR );
        ItemType type = ItemType.valueOf( properties.getProperty( Constants.ITEM_TYPE ) );
        int queueSize = Integer.parseInt( properties.getProperty( Constants.QUEUE_SIZE ) );
        boolean ascending = Boolean.valueOf( properties.getProperty( Constants.ASCENDING ) );

        CloseableQueue<List<Item<?>>> q = new CloseableQueue<>( queueSize );
        ShardFile sf = new ShardFile( type, file, q, properties );
        String newBase = prepare( file, outputDir );

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

    }

    private String prepare( String file, String outputDir )
    {
        String[] parts = Util.splitPathAndFilename( file );
        Util.createDir( outputDir );
        String newBase = Util.createNewBaseFile( outputDir, parts[1] );

        return newBase;
    }

}

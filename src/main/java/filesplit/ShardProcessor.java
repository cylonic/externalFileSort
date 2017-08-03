package filesplit;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Util;

public class ShardProcessor
{

    public static void main( String[] args )
    {
        ShardProcessor pro = new ShardProcessor();
        pro.start();
    }

    private void start()
    {
        String file = "/data/fluffy.txt";
        CloseableQueue<List<Item<?>>> q = new CloseableQueue<>();
        ShardFile sf = new ShardFile( ItemType.INTEGER, file, q );
        String newBase = prepare( file );

        ShardWriter sw = new ShardWriter( true, newBase, q );

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

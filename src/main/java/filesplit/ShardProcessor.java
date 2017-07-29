package filesplit;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import model.CloseableListQueue;
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
        CloseableListQueue q = new CloseableListQueue();
        ShardFile sf = new ShardFile( ItemType.INTEGER, file, q );
        ShardWriter sw = new ShardWriter( true, Util.stripFileExt( file ) + "_", q );

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

}

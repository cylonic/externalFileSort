package processor;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import util.Util;

public class Processor
{

    public Processor()
    {
    }

    private static Properties props = Util.getDefaultProps();
    private static ExecutorService service = Executors.newFixedThreadPool( 3 );
    private static CompletionService<Integer> completionService = new ExecutorCompletionService<>( service );

    public static void main( String[] args )
    {

        try
        {
            Queue<Path> files = Util.prefixedFiles( "/data/shards/", "*" );
            while ( files.size() > 1 )
            {
                createAndStartTasks( files );

                files = Util.prefixedFiles( "/data/shards/", "*" );

            }

        } finally
        {
            service.shutdown();
            System.out.println( "Processor status : DONE" );
        }
    }

    private static void createAndStartTasks( Queue<Path> files )
    {
        try
        {

            List<Callable<Integer>> tasks = getTasks( files );
            if ( tasks.isEmpty() )
            {
                System.out.println( "No files to merge" );
                return;
            }

            startAndWaitForTasks( tasks );

        } catch ( InterruptedException e )
        {
            String msg = "Critical error, a thread was interrupted";
            Thread.currentThread().interrupt();
            throw new RuntimeException( msg, e );
        }
    }

    private static List<Callable<Integer>> getTasks( Queue<Path> files )
    {
        List<Callable<Integer>> c = new ArrayList<>();

        while ( files.size() > 1 )
        {
            String p1 = files.poll().toAbsolutePath().toString();
            String p2 = files.poll().toAbsolutePath().toString();
            String merged = "/data/shards/merged_output" + ( files.size() + "" + Instant.now().toEpochMilli() );
            c.add( new LargeFileSortProcessor( props, p1, p2, merged + ".txt" ) );
        }

        return c;
    }

    private static void startAndWaitForTasks( List<Callable<Integer>> tasks ) throws InterruptedException
    {
        int futures = 0;
        for (Callable<Integer> c : tasks)
        {
            futures++;
            completionService.submit( c );
        }

        Future<Integer> future;

        while ( futures > 0 )
        {
            future = completionService.take();
            futures--;

            try
            {
                future.get();
            } catch ( ExecutionException ee )
            {
                ee.printStackTrace();
                continue;
            }
        }
    }

}

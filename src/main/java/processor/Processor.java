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

import filesplit.ShardProcessor;
import util.Constants;
import util.Util;

public class Processor
{

    private static Properties props = Util.getDefaultProps();
    private static ExecutorService service = Executors.newFixedThreadPool( 3 );
    private static CompletionService<Integer> completionService = new ExecutorCompletionService<>( service );

    private static String SHARD_DIR;
    private static String MERGING_WILDCARD;

    public static void main( String[] args )
    {
        gatherArgs( args );
        initVars();

        runShardProcessor();
        runMerge();
    }

    private static void runShardProcessor()
    {
        ShardProcessor sp = new ShardProcessor( props );
        sp.start();
    }

    private static void runMerge()
    {
        try
        {
            Queue<Path> files = Util.prefixedFiles( SHARD_DIR, MERGING_WILDCARD );

            if ( files == null || files.isEmpty() )
            {
                System.out.println( "No files to merge found" );
                return;
            }

            while ( files.size() > 1 )
            {
                createAndStartTasks( files );

                files = Util.prefixedFiles( SHARD_DIR, MERGING_WILDCARD );
            }

        } finally
        {
            service.shutdown();
            System.out.println( "Processor status : DONE" );
        }
    }

    private static void initVars()
    {
        SHARD_DIR = props.getProperty( Constants.SHARD_OUTPUT_DIR );
        MERGING_WILDCARD = props.getProperty( Constants.MERGING_WILDCARD );
    }

    private static void gatherArgs( String[] args )
    {
        for (String arg : args)
        {
            String[] parts = arg.split( "=" );
            props.put( parts[0], parts[1] );
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
            String merged = SHARD_DIR + ( files.size() + "" + Instant.now().toEpochMilli() );
            c.add( new LargeFileSortProcessor( props, p1, p2, merged + ".txt" ) );
        }

        return c;
    }

    private static void startAndWaitForTasks( List<Callable<Integer>> tasks ) throws InterruptedException
    {
        List<Future<Integer>> futures = new ArrayList<>();
        for (Callable<Integer> c : tasks)
        {
            futures.add( completionService.submit( c ) );
        }

        Future<Integer> completed;

        while ( futures.size() > 0 )
        {
            completed = completionService.take();
            futures.remove( completed );

            try
            {
                completed.get();
            } catch ( ExecutionException ee )
            {

                for (Future<Integer> f : futures)
                {
                    f.cancel( true );
                }

                String msg = "Error during a threads execution. All threads stopped";
                throw new RuntimeException( msg, ee );
            }
        }
    }

}

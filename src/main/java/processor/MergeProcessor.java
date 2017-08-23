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

import util.Constants;
import util.Util;

public class MergeProcessor
{

    private Properties props;
    private String SHARD_DIR;
    private String MERGING_WILDCARD;
    private ExecutorService service;
    private CompletionService<Integer> completionService;

    public MergeProcessor( Properties properties )
    {
        this.props = properties;
        int mergingThreads = Integer.parseInt( props.getProperty( Constants.MERGING_THREADS ) );
        service = Executors.newFixedThreadPool( mergingThreads );
        completionService = new ExecutorCompletionService<>( service );

        SHARD_DIR = props.getProperty( Constants.SHARD_OUTPUT_DIR );
        MERGING_WILDCARD = props.getProperty( Constants.MERGING_WILDCARD );
    }

    public void start()
    {
        runMerge();
    }

    private void runMerge()
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

    private void createAndStartTasks( Queue<Path> files )
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

    private List<Callable<Integer>> getTasks( Queue<Path> files )
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

    private void startAndWaitForTasks( List<Callable<Integer>> tasks ) throws InterruptedException
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

package processor;

import java.nio.file.Path;
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
import java.util.concurrent.ThreadLocalRandom;

import util.Util;

public class Processor
{

    public Processor()
    {
        // TODO Auto-generated constructor stub
    }

    private static Properties props = Util.getDefaultProps();
    private static ExecutorService service = Executors.newFixedThreadPool( 3 );
    private static CompletionService<Integer> completionService = new ExecutorCompletionService<>( service );

    public static void main( String[] args )
    {
        // Properties props = Util.getDefaultProps();
        // ExecutorService service = Executors.newFixedThreadPool( 3 );
        // CompletionService completionService = new
        // ExecutorCompletionService<>( service );

        try
        {

            List<Callable<Integer>> tasks = getTasks();
            if ( tasks.isEmpty() )
            {
                System.out.println( "No files to merge" );
                return;
            }

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

        } catch ( InterruptedException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally
        {
            service.shutdown();
            System.out.println( "Processor status : DONE" );
        }
    }

    private static List<Callable<Integer>> getTasks()
    {
        Queue<Path> files = Util.prefixedFiles( "/data/shards/", "*" );
        List<Callable<Integer>> c = new ArrayList<>();

        while ( files.size() > 1 )
        {
            String p1 = files.poll().toAbsolutePath().toString();
            String p2 = files.poll().toAbsolutePath().toString();
            String merged = "/data/shards/merged_output"
                    + ( String.valueOf( ThreadLocalRandom.current().nextInt( 1_000_000 ) ) );
            c.add( new LargeFileSortProcessor( props, p1, p2, merged + ".txt" ) );
        }

        return c;
    }

}

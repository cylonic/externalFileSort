package processor;

import java.util.Properties;

import util.Util;

public class Processor
{

    private static Properties props = Util.getDefaultProps();

    public static void main( String[] args )
    {
        try
        {
            gatherArgs( args );

            runShardProcessor();
            runMerge();
        } catch ( Throwable t )
        {
            String msg = "Caught an unforseen error during processing";
            throw new RuntimeException( msg, t );

        }
    }

    private static void gatherArgs( String[] args )
    {
        if ( args.length != 0 )
        {
            System.out.println( "Overriding properties from file with command line args!" );
        }
        for (String arg : args)
        {
            String[] parts = arg.split( "=" );
            props.put( parts[0], parts[1] );
        }
    }

    private static void runShardProcessor()
    {
        System.out.println( "Shard Processor started - creating shards." );
        ShardProcessor sp = new ShardProcessor( props );
        sp.start();
        System.out.println( "Shard Processor finished." );
    }

    private static void runMerge()
    {
        System.out.println( "Merge Processor started - merging all shards" );
        MergeProcessor mp = new MergeProcessor( props );
        mp.start();
        System.out.println( "Merge Processor finished." );

    }

}

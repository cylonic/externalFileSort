package processor;

import java.util.Properties;

import util.Util;

public class Processor
{

    private static Properties props = Util.getDefaultProps();

    public static void main( String[] args )
    {
        gatherArgs( args );

        runShardProcessor();
        runMerge();
    }

    private static void gatherArgs( String[] args )
    {
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

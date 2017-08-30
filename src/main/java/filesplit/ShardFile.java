package filesplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import datasource.Datasource;
import datasource.InputDatasource;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;
import util.Constants;

public class ShardFile implements Runnable
{

    private String file;
    private ItemType type;
    private CloseableQueue<List<Item<?>>> q;
    private Properties properties;
    private int shardSize;

    public ShardFile( ItemType type, String file, CloseableQueue<List<Item<?>>> q, Properties props )
    {
        this.type = type;
        this.file = file;
        this.q = q;
        this.properties = props;
        this.shardSize = Integer.parseInt( properties.getProperty( Constants.SHARD_SIZE ) );
    }

    public void splitFile()
    {
        Datasource ds = new InputDatasource<>( file, type );
        try
        {
            int count = 1;
            Item<?> item;
            List<Item<?>> items = new ArrayList<>();
            while ( ( item = ds.getNextItem() ) != null )
            {
                items.add( item );

                if ( items.size() >= shardSize )
                {
                    q.put( items );
                    items = new ArrayList<>();
                }

            }
        } catch ( IOException e )
        {
            e.printStackTrace();
        } finally
        {
            q.close();
            ds.close();
        }

    }

    @Override
    public void run()
    {
        splitFile();

    }

}

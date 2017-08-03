package filesplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import datasource.Datasource;
import model.CloseableQueue;
import model.Item;
import model.Item.ItemType;

public class ShardFile implements Runnable
{
    private String file;
    private ItemType type;
    private CloseableQueue<List<Item<?>>> q;

    public ShardFile( ItemType type, String file, CloseableQueue<List<Item<?>>> q )
    {
        this.type = type;
        this.file = file;
        this.q = q;
    }

    public void splitFile()
    {
        Datasource<?> ds = new Datasource<>( file, type );
        try
        {
            int count = 1;
            Item<?> item;
            List<Item<?>> items = new ArrayList<>();
            while ( ( item = ds.getNextItem() ) != null )
            {
                items.add( item );

                if ( items.size() >= 1_000_000 )
                {
                    System.out.println( "Read " + ( ++count ) * 1_000_000 + " records" );
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
        System.out.println( "splitFile done" );

    }

}

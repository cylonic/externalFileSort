package merge;

import java.io.IOException;

import datasource.Datasource;
import model.CloseableQueue;
import model.Item;

public abstract class Merge
{
    CloseableQueue<Item<?>> queue;

    public Merge( CloseableQueue<Item<?>> queue )
    {
        this.queue = queue;
    }

    abstract void merge( Datasource d1, Datasource d2 ) throws IOException;

    void runFileOut( Datasource d ) throws IOException
    {
        Item<?> item;
        while ( ( item = d.getNextItem() ) != null )
        {
            queue.put( item );
        }
    }

}

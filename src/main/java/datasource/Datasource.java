package datasource;

import java.io.IOException;

import factory.ItemFactory;
import io.FileReader;
import model.Item;
import model.Item.ItemType;
import util.Util;

public class Datasource<E>
{
    private FileReader reader;
    private final ItemFactory factory;

    public Datasource( String fullPathAndFile, ItemType type )
    {
        this.reader = new FileReader( fullPathAndFile );
        this.factory = Util.getFactory( type );

        if ( factory == null )
        {
            String msg = "Unknown ItemType";
            throw new RuntimeException( msg );
        }
    }

    public Item<?> getNextItem() throws IOException
    {
        String line;
        if ( ( line = reader.getNextString() ) != null )
        {
            return factory.getNewItem( line );
        }
        return null;
    }

    public void close()
    {
        reader.close();
    }

}

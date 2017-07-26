package factory;

import model.Item;
import model.Item.ItemType;

public class StringItemFactory implements ItemFactory
{

    private final ItemType type = ItemType.STRING;

    @Override
    public Item<String> getNewItem( String line )
    {
        return new Item<String>( line, this.type );
    }

}

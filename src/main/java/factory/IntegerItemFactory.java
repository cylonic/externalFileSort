package factory;

import model.Item;
import model.Item.ItemType;

public class IntegerItemFactory implements ItemFactory
{

    private final ItemType type = ItemType.INTEGER;

    @Override
    public Item<Integer> getNewItem( String line )
    {
        return new Item<Integer>( Integer.parseInt( line ), type );
    }

}

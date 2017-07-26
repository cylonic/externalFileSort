package factory;

import model.Item;
import model.Item.ItemType;

public class DoubleItemFactory implements ItemFactory
{

    private final ItemType type = ItemType.DOUBLE;

    @Override
    public Item<Double> getNewItem( String line )
    {
        return new Item<Double>( Double.parseDouble( line ), type );
    }

}

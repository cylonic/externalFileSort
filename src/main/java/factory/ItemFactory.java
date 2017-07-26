package factory;

import model.Item;

public interface ItemFactory
{

    Item<?> getNewItem( String line );

}

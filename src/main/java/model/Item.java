package model;

public class Item<E>
{
    public enum ItemType
    {
        STRING, INTEGER, DOUBLE
    };

    private E item;
    private ItemType type;

    public Item( E item, ItemType type )
    {
        this.item = item;
        this.type = type;
    }

    public E getItem()
    {
        return item;
    }

    public ItemType getType()
    {
        return type;
    }

    public int compareTo( Item<?> o )
    {
        if ( this.type != o.getType() )
        {
            throw new RuntimeException( "Cant compare differntly typed objects" );
        }

        switch ( this.type )
        {
        case STRING:
            String o1 = (String) this.item;
            String o2 = (String) o.getItem();
            return o1.compareTo( o2 );
        case INTEGER:
            Integer i1 = (Integer) this.item;
            Integer i2 = (Integer) o.getItem();
            return i1.compareTo( i2 );
        case DOUBLE:
            Double d1 = (Double) this.item;
            Double d2 = (Double) o.getItem();
            return d1.compareTo( d2 );
        default:
            throw new RuntimeException( "Failed to compare items" );

        }

    }

}

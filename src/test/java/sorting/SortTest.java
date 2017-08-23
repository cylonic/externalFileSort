package sorting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import junit.framework.Assert;
import model.Item;
import model.Item.ItemType;

public class SortTest
{

    public SortTest()
    {
        // TODO Auto-generated constructor stub
    }

    @SuppressWarnings( "deprecation" )
    @Test
    public void testSortedAscList()
    {
        ItemType type = ItemType.INTEGER;

        List<Item<?>> ints = Arrays.asList( new Item<Integer>( 1, type ), new Item<Integer>( 2, type ),
                new Item<Integer>( 5, type ), new Item<Integer>( 3, type ), new Item<Integer>( 7, type ),
                new Item<Integer>( 44, type ), new Item<Integer>( 1, type ), new Item<Integer>( 55, type ),
                new Item<Integer>( 901, type ), new Item<Object>( -5, type ), new Item<Integer>( 32, type ),
                new Item<Integer>( 0, type ) );

        List<Integer> result = new ArrayList<>();

        Sort.sortList( ints, true ).forEach( s -> result.add( (Integer) s.getItem() ) );

        Assert.assertEquals( Arrays.asList( -5, 0, 1, 1, 2, 3, 5, 7, 32, 44, 55, 901 ), result );

    }

}

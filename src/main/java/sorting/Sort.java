package sorting;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import model.Item;

public class Sort
{

    public static List<Item<?>> sortList( List<Item<?>> items, boolean ascending )
    {
        return getSortedStream( items, ascending ).collect( Collectors.toList() );
    }

    private static Stream<Item<?>> getSortedStream( List<Item<?>> list, boolean ascending )
    {
        Comparator<Item<?>> c;

        if ( ascending )
        {
            c = ( i1, i2 ) -> i1.compareTo( i2 );
        } else
        {
            c = ( i1, i2 ) -> i2.compareTo( i1 );
        }

        return list.stream().sorted( c );

    }

}

package sorting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.FileWriter;
import model.Item;
import model.Item.ItemType;

/**
 * 
 * Temp testing file
 *
 */
public class Sort
{
    public static void main( String[] args )
    {
        // sort();
        itemTest();
    }

    public Sort()
    {
        // TODO Auto-generated constructor stub
    }

    private static void itemTest()
    {
        Item<String> item = new Item<>( "99500", ItemType.STRING );
        Item<String> item2 = new Item<>( "99595", ItemType.STRING );

        System.out.println( item.compareTo( item2 ) );

        System.out.println( "99500".compareTo( "99595" ) );
    }

    public static void sort()
    {
        List<Integer> ints = new ArrayList<>();

        for (int i = 0; i < 10_000; i++)
        {
            ints.add( ThreadLocalRandom.current().nextInt( 1_000_000 ) );
        }

        ints = getStream( ints, true ).collect( Collectors.toList() );

        FileWriter fw = new FileWriter( "/data/fluffy.txt" );

        for (Integer line : ints)
        {
            try
            {
                fw.write( line.toString() );
            } catch ( IOException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        fw.close();

    }

    public static Stream<Integer> getStream( List<Integer> list, boolean ascending )
    {

        if ( ascending )

        {
            return list.stream().sorted();
        }

        return list.stream().sorted( Comparator.reverseOrder() );

    }

}

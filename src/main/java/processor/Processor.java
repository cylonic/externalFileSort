package processor;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Processor
{

    public Processor()
    {
        // TODO Auto-generated constructor stub
    }

    public static void main( String[] args )
    {
        Properties props = new Properties();
        InputStream is = null;

        try
        {
            // is = new FileInputStream( "default.properties" );
            is = Processor.class.getClassLoader().getResourceAsStream( "props/default.properties" );
            props.load( is );
        } catch ( FileNotFoundException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( IOException e )
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally
        {
            if ( is != null )
            {
                try
                {
                    is.close();
                } catch ( IOException e )
                {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        System.out.println( props.toString() );
    }

}

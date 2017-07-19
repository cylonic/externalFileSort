package processor;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import io.FileReader;
import model.CloseableQueue;

public class QueueBuilder implements Runnable
{
    private CloseableQueue queue;

    public QueueBuilder( final CloseableQueue q )
    {
        this.queue = q;
    }

    @Override
    public void run()
    {
        buildQueue();
    }

    public void buildQueue()
    {

        FileReader reader = null;
        FileReader reader2 = null;

        try
        {
            reader = new FileReader( "src/main/resources/files/File_3.txt" );
            reader2 = new FileReader( "src/main/resources/files/File_4.txt" );

            String a = reader.getNextLine();
            String b = reader2.getNextLine();

            labelParent: while ( true )
            {

                if ( a == null && b == null )
                {
                    break;
                }

                if ( a == null )
                {
                    queue.put( b );
                    b = reader2.getNextLine();
                    continue;
                }

                if ( b == null )
                {

                    queue.put( a );
                    a = reader.getNextLine();
                    continue;
                }

                while ( a.compareTo( b ) > 0 )
                {
                    queue.put( b );
                    String tempB = reader2.getNextLine();

                    if ( tempB == null ) // reader 2 file is empty
                    {
                        queue.put( a );
                        while ( ( a = reader.getNextLine() ) != null )
                        {
                            queue.put( a );
                        }
                        return; // both files should be empty at this
                                // point
                    }

                    if ( a.compareTo( tempB ) < 0 )
                    {
                        queue.put( a );
                        a = reader.getNextLine();
                        b = tempB;
                        continue labelParent;
                    }
                    b = tempB;

                }

                while ( b.compareTo( a ) > 0 )
                {
                    queue.put( a );
                    String tempA = reader.getNextLine();

                    if ( tempA == null ) // reader 1 file is empty
                    {
                        queue.put( b );
                        while ( ( b = reader2.getNextLine() ) != null )
                        {
                            queue.put( b );
                        }
                        return; // both files should be empty at this
                                // point
                    }

                    if ( b.compareTo( a ) < 0 )
                    {
                        queue.put( b );
                        b = reader2.getNextLine();
                        a = tempA;
                        continue labelParent;
                    }
                    a = tempA;

                }

                if ( a.compareTo( b ) == 0 )
                {
                    queue.put( a );
                    queue.put( b );
                    a = reader.getNextLine();
                    b = reader2.getNextLine();
                }

            }

        } catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( "Improper encoding", e );
        } catch ( IOException e )
        {
            throw new RuntimeException( "Couldnt open file ", e );
        } finally
        {
            reader.close();
            reader2.close();
            queue.close();
        }

        return;

    }

}

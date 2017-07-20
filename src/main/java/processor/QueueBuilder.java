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

        FileReader leftReader = null;
        FileReader rightReader = null;

        try
        {
            leftReader = new FileReader( "src/main/resources/files/File_3.txt" );
            rightReader = new FileReader( "src/main/resources/files/File_4.txt" );

            String left = leftReader.getNextLine();
            String right = rightReader.getNextLine();

            labelParent: while ( true )
            {

                if ( left == null && right == null )
                {
                    break;
                }

                if ( left == null )
                {
                    queue.put( right );
                    right = rightReader.getNextLine();
                    continue;
                }

                if ( right == null )
                {
                    queue.put( left );
                    left = leftReader.getNextLine();
                    continue;
                }

                while ( left.compareTo( right ) > 0 )
                {
                    queue.put( right );
                    String tempRight = rightReader.getNextLine();

                    if ( tempRight == null ) // reader 2 file is empty
                    {
                        queue.put( left );
                        runFileOut( leftReader );
                        return; // both files should be empty at this point
                    }

                    if ( left.compareTo( tempRight ) < 0 )
                    {
                        queue.put( left );
                        left = leftReader.getNextLine();
                        right = tempRight;
                        continue labelParent;
                    }
                    right = tempRight;

                }

                while ( right.compareTo( left ) > 0 )
                {
                    queue.put( left );
                    String tempLeft = leftReader.getNextLine();

                    if ( tempLeft == null ) // reader 1 file is empty
                    {
                        queue.put( right );
                        runFileOut( rightReader );
                        return; // both files should be empty at this point
                    }

                    if ( right.compareTo( left ) < 0 )
                    {
                        queue.put( right );
                        right = rightReader.getNextLine();
                        left = tempLeft;
                        continue labelParent;
                    }
                    left = tempLeft;

                }

                if ( left.compareTo( right ) == 0 )
                {
                    queue.put( left );
                    queue.put( right );
                    left = leftReader.getNextLine();
                    right = rightReader.getNextLine();
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
            leftReader.close();
            rightReader.close();
            queue.close();
        }

        return;

    }

    private void runFileOut( FileReader r ) throws IOException
    {
        String line;
        while ( ( line = r.getNextLine() ) != null )
        {
            queue.put( line );
        }
    }

}

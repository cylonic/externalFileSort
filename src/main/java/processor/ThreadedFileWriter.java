package processor;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import io.FileWriter;
import model.CloseableQueue;

public class ThreadedFileWriter implements Runnable
{
    private CloseableQueue q;
    private FileWriter writer;

    public ThreadedFileWriter( final CloseableQueue queue, final String pathAndFileName )
    {
        this.q = queue;
        this.writer = new FileWriter( pathAndFileName );
    }

    @Override
    public void run()
    {
        try
        {
            String line;
            while ( !( q.isClosed() && q.isEmpty() ) )
            {
                line = q.poll( 2, TimeUnit.SECONDS );

                if ( null == line )
                {
                    continue;
                }
                writer.write( line );
            }
        } catch ( IOException e )
        {
            throw new RuntimeException( "Error writing to file" );
        } catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Thread interrupted", e );
        } finally
        {
            writer.close();
        }
    }

}

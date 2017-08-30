package datasource;

import java.io.IOException;

import model.Item;

public interface Datasource
{

    Item<?> getNextItem() throws IOException;

    void close();

}

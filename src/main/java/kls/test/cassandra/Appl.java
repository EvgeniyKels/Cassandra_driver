package kls.test.cassandra;
import static kls.test.cassandra.IConstants.*;

public class Appl {
    public static void main(String[] args) {
        CassandraConnector cassandraConnector =
                new CassandraConnector();
        cassandraConnector.connect(connectionAddress, connectionPort);
        Cassandra_CR_Repository cassandraCRRepository = new Cassandra_CR_Repository(cassandraConnector.getSession());

        cassandraCRRepository.createKeySpace();
        cassandraCRRepository.createTable();
        cassandraCRRepository.insertResellers();
        cassandraCRRepository.insertClicks();
    }
}

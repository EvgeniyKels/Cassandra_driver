//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Session;
//import com.datastax.driver.core.utils.UUIDs;
//import kls.test.cassandra.Book;
//import kls.test.cassandra.CassandraConnector;
//import kls.test.cassandra.CassandraRepository;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.List;
//import java.util.stream.Collectors;
//
//import static kls.test.cassandra.IConstants.*;
//import static org.junit.Assert.*;
//
//public class CreationTests {
//    private Session session;
//    private CassandraRepository cassandraRepository;
//    @Before
//    public void connect() {
//        CassandraConnector connector = new CassandraConnector();
//        connector.connect(connectionAddress, connectionPort);
//        session = connector.getSession();
//        cassandraRepository = new CassandraRepository(session);
//    }
//
//    @Test
//    public void createNameSpace() {
//        cassandraRepository.createKeySpace(keySpaceName, replicationStrategy, replicationFactor);
//        ResultSet resultSet = session.execute("SELECT * FROM system_schema.keyspaces;");
//        List <String> keySpaces = resultSet.all()
//                .stream()
//                .filter(row -> row.getString(0).equals(keySpaceName.toLowerCase()))
//                .map(row -> row.getString(0))
//                .collect(Collectors.toList());
//        assertEquals(keySpaces.size(), 1);
//        assertEquals(keySpaces.get(0), keySpaceName.toLowerCase());
//    }
//
//    @Test
//    public void createTable() {
//        cassandraRepository.createTable(keySpaceName, tableName);
//        ResultSet result = session.execute(
//                "SELECT * FROM " + keySpaceName + ".books;");
//
//        List<String> columnNames =
//                result.getColumnDefinitions().asList().stream()
//                        .map(column -> column.getName())
//                        .collect(Collectors.toList());
//
//        assertEquals(columnNames.size(), 3);
//        assertTrue(columnNames.contains("id"));
//        assertTrue(columnNames.contains("title"));
//        assertTrue(columnNames.contains("subject"));
//    }
//
//    @Test
//    public void alterTable() {
//        cassandraRepository.createTable(keySpaceName, tableName);
//        cassandraRepository.alterTablebooks(tableName, columnName, columnType);
//        ResultSet result = session.execute(
//                "SELECT * FROM " + keySpaceName + "." + tableName + ";");
//        boolean columnExists = result.getColumnDefinitions().asList().stream()
//                .anyMatch(cl -> cl.getName().equals(columnName));
//        assertTrue(columnExists);
//    }
//
//    @Test
//    public void whenAddingANewBook_thenBookExists() {
//        cassandraRepository.createTableBooksByTitle(keySpaceName);
//        String title = "Effective Java";
//        Book book = new Book(UUIDs.timeBased().toString(), title, "Programming");
//        cassandraRepository.insertBookByTitle(book, tableName);
//        Book savedBook = cassandraRepository.selectByTitle(title);
//        assertEquals(book.getTitle(), savedBook.getTitle());
//    }
//}
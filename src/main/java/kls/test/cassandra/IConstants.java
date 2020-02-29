package kls.test.cassandra;

public interface IConstants {
    String connectionAddress = "127.0.0.1";
    int connectionPort = 9042;
    String keySpaceName = "library";
    String replicationStrategy = "SimpleStrategy";
    int replicationFactor = 1;
    String tableName = "books";
    String columnName = "publisher";
    String columnType = "text";
}

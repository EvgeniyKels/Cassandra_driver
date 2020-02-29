package kls.test.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.Arrays;
import java.util.List;

public class Cassandra_CR_Repository {
    private Session session;
    Cassandra_CR_Repository(Session session) {
        this.session = session;
    }
    void createKeySpace(/*String keySpaceName, String replicationStrategy, int replicationFactor*/) {
        String createKeySpace =
                "CREATE KEYSPACE IF NOT EXISTS ad_network " +
                        "WITH replication = " +
                        "{'class': 'SimpleStrategy'," +
                        " 'replication_factor': '3'};";
        executeQuery(createKeySpace);
        executeQuery("USE ad_network");
    }
    void createTable() {
        // id - raspredelit. partition key - define node
        // effective_since - cluster key - define row
        String createTable
                = "CREATE TABLE IF NOT EXISTS reseller " +
                " (id text," +
                " effective_since text," +
                " reward_percent float," +
                " PRIMARY KEY (id, effective_since))" +
                " WITH CLUSTERING ORDER BY (effective_since DESC);";
        executeQuery(createTable);
    }
    void insertResellers() {
//            node              row
//        Map <id, < Map <effective_since, List<resellers>>>
        String firstInsert = "INSERT INTO reseller (id, effective_since, reward_percent) VALUES ('supaboobs', '2011-02-13', 0.2);";
        String secondInsert = "INSERT INTO reseller (id, effective_since, reward_percent) VALUES ('supaboobs', '2012-01-22', 0.25);";
        String thirdInsert = "INSERT INTO reseller (id, effective_since, reward_percent) VALUES ('supaboobs', '2013-11-30', 0.3);";
        executeQuery(firstInsert);
        executeQuery(secondInsert);
        executeQuery(thirdInsert);

        System.out.println("from ------------- reseller -------------------");
        String selectAll = "SELECT * FROM reseller WHERE id='supaboobs';";
        ResultSet resultSet = executeQuery(selectAll);
        resultSet.all().forEach(x -> System.out.println(x.toString()));
    }

    void insertClicks() {
        String createTable = "CREATE TABLE IF NOT EXISTS ad_click"
                + " (reseller_id text,"
                + " day text,"
                + " time timestamp,"
                + " ad_id text,"
                + " amount float,"
                + " PRIMARY KEY ((reseller_id, day), time, ad_id))"
//          распределительный ключ (reseller_id, day) и кластерные ключи (time, ad_id)
                + " WITH CLUSTERING ORDER BY (time DESC);";
        executeQuery(createTable);

        List <String> inserts = Arrays.asList(
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-11-28', '2013-11-28 02:16:52', '890_567_234', 0.005);",
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-11-28', '2013-11-28 07:17:35', '890_567_234', 0.005);",
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-11-29', '2013-11-29 17:18:51', '890_567_211', 0.0075);",
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-11-29', '2013-11-29 22:20:37', '890_567_211', 0.0075);",
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-11-30', '2013-11-30 11:21:56', '890_567_234', 0.005);",
                "INSERT INTO ad_click (reseller_id, day, time, ad_id, amount) VALUES ('supaboobs', '2013-12-01', '2013-12-01 12:21:59', '890_567_010', 0.01);"
        );
        for (String s:
             inserts) {
            executeQuery(s);
        }
        System.out.println("from ------------- ad_click -------------------");
        String selectAll = "SELECT * FROM ad_click;";
        ResultSet resultSet = executeQuery(selectAll);
        resultSet.all().forEach(x -> System.out.println(x.toString()));
    }

    private ResultSet executeQuery(String query) {
        return session.execute(query);
    }

}

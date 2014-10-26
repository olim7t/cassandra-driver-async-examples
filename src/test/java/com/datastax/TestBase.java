package com.datastax;

import java.util.UUID;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class TestBase {
    protected Cluster cluster;
    protected Session session;

    @BeforeClass
    public void setup() {
        cluster = Cluster.builder()
            .addContactPoint("127.0.0.1")
            .build();
        session = cluster.connect();

        session.execute("CREATE KEYSPACE IF NOT EXISTS async_examples " +
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("USE async_examples");

        session.execute("CREATE TABLE IF NOT EXISTS users (id uuid PRIMARY KEY, name text)");

        session.execute("INSERT INTO users (id, name) VALUES (e6af74a8-4711-4609-a94f-2cbfab9695e5, 'user1')");
        session.execute("INSERT INTO users (id, name) VALUES (281336f4-2a52-4535-847c-11a4d3682ec1, 'user2')");
        session.execute("INSERT INTO users (id, name) VALUES (c32b8d37-89bd-4dfe-a7d5-5f0258692d05, 'user3')");
        session.execute("INSERT INTO users (id, name) VALUES (973fe99f-5715-4dfd-a28d-5b3751b26ab5, 'user4')");
        session.execute("INSERT INTO users (id, name) VALUES (0aabb840-bab6-474b-9f08-c18527a2b47f, 'user5')");
    }

    @AfterClass
    public void teardown() {
        if (cluster != null)
            cluster.close();
    }

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            System.out.println(UUID.randomUUID());
        }
    }
}

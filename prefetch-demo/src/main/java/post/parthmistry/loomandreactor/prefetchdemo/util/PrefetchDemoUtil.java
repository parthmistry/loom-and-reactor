package post.parthmistry.loomandreactor.prefetchdemo.util;

import io.r2dbc.postgresql.PostgresqlConnectionConfiguration;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import post.parthmistry.loomandreactor.prefetchdemo.data.PersonData;
import reactor.core.publisher.Mono;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PrefetchDemoUtil {

    public static Connection getConnection() throws SQLException {
        var jdbcUrl = "jdbc:postgresql://localhost:9090/demo";
        var username = "postgres";
        var password = "adminTest@123";
        var connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(false);
        return connection;
    }

    public static Mono<? extends io.r2dbc.spi.Connection> getR2dbcConnection() throws SQLException {
        var postgresqlConfig = PostgresqlConnectionConfiguration.builder()
                .host("localhost")
                .port(9090)
                .username("postgres")
                .password("adminTest@123")
                .database("demo")
                .build();
        var connectionFactory = new PostgresqlConnectionFactory(postgresqlConfig);
        return Mono.from(connectionFactory.create());
    }

    public static PersonData createPersonData(ResultSet resultSet) throws SQLException {
        int id = resultSet.getInt("id");
        String name = resultSet.getString("name");
        return new PersonData(id, name);
    }

}

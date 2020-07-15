package resourceleak;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestCheckIsClosed {

    private void testCheckIsClosed(String query) throws SQLException {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        try {
            stmt.execute(query);
        } finally {
            if (!stmt.isClosed()) {
                stmt.close();
            }
        }
    }
}

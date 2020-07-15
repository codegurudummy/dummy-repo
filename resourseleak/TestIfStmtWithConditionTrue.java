package resourceleak;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestIfStmtWithConditionTrue {

    private void testIfStmtWithConditionTrue(String query) throws SQLException {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        if (true) {
            try {
                stmt.execute(query);
            } finally {
                if (!stmt.isClosed()) {
                    stmt.close();
                }
            }
        }
    }
}

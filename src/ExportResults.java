import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ExportResults {

    private final String url = "jdbc:sqlite:database/ImpresionMiner.db";

    //Export keyword search results to .csv file.
    public void exportKeywordsFile(){
        /**
         * Write to a file
         */

        String query = "SELECT k.id AS id, u.url AS url, k.keyword AS keyword, k.shown AS count, k.timestamp AS Date FROM keywords AS k INNER JOIN urls AS u ON k.url_id = u.id;";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            FileWriter fileWriter = new FileWriter("Keywords-Results.csv")) {

            fileWriter.append("id");
            fileWriter.append(';');
            fileWriter.append("url");
            fileWriter.append(';');
            fileWriter.append("keyword");
            fileWriter.append(';');
            fileWriter.append("count");
            fileWriter.append(';');
            fileWriter.append("Date");
            fileWriter.append('\n');
            while (rs.next()) {
                fileWriter.append(rs.getString(1));
                fileWriter.append(';');
                fileWriter.append(rs.getString(2));
                fileWriter.append(';');
                fileWriter.append(rs.getString(3));
                fileWriter.append(';');
                fileWriter.append(rs.getString(4));
                fileWriter.append(';');
                fileWriter.append(rs.getString(5));
                fileWriter.append(';');
                fileWriter.append('\n');
            }
            fileWriter.flush();

            System.out.println("==================================================================");
            System.out.println("SUCCESS: Keywords CSV File was created successfully.");
            System.out.println("==================================================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Export negative impressions found in keyword searches to .csv file.
    public void exportNegativeFile(){
        /**
         * Write to a file
         */
        String query = "SELECT i.id AS id, u.url AS url, i.keywords_id AS keywords_table_id ,  k.keyword AS keyword,  t.term AS negative_term, i.timestamp AS Date FROM negativeimpressions AS i " +
                "INNER JOIN negativeterms AS t ON i.term_id = t.id " +
                "INNER JOIN urls AS u ON i.url_id = u.id " +
                "INNER JOIN keywords AS k ON i.keywords_id = k.id;";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            FileWriter fileWriter = new FileWriter("Negative-Results.csv")) {

            fileWriter.append("id");
            fileWriter.append(';');
            fileWriter.append("url");
            fileWriter.append(';');
            fileWriter.append("keywords_table_id");
            fileWriter.append(';');
            fileWriter.append("keyword");
            fileWriter.append(';');
            fileWriter.append("negative_term");
            fileWriter.append(';');
            fileWriter.append("Date");
            fileWriter.append('\n');
            while (rs.next()) {
                fileWriter.append(rs.getString(1));
                fileWriter.append(';');
                fileWriter.append(rs.getString(2));
                fileWriter.append(';');
                fileWriter.append(rs.getString(3));
                fileWriter.append(';');
                fileWriter.append(rs.getString(4));
                //System.out.println(rs.getString(4));    //Test
                fileWriter.append(';');
                fileWriter.append(rs.getString(5));
                fileWriter.append(';');
                fileWriter.append(rs.getString(6));
                fileWriter.append(';');
                fileWriter.append('\n');

            }
            fileWriter.flush();

            System.out.println("==================================================================");
            System.out.println("SUCCESS: NegativeImpressions CSV File was created successfully.");
            System.out.println("==================================================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Export positive impressions found in keyword searches to .csv file.
    public void exportPositiveFile(){
        /**
         * Write to a file
         */
        String query = "SELECT i.id AS id, u.url AS url, i.keywords_id AS keywords_table_id,  k.keyword AS keyword,  t.term AS positive_term, i.timestamp AS Date FROM positiveimpressions AS i " +
                "INNER JOIN positiveterms AS t ON i.term_id = t.id " +
                "INNER JOIN urls AS u ON i.url_id = u.id " +
                "INNER JOIN keywords AS k ON i.keywords_id = k.id;";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            FileWriter fileWriter = new FileWriter("Positive-Results.csv")) {

            fileWriter.append("id");
            fileWriter.append(';');
            fileWriter.append("url");
            fileWriter.append(';');
            fileWriter.append("keywords_table_id");
            fileWriter.append(';');
            fileWriter.append("keyword");
            fileWriter.append(';');
            fileWriter.append("positive_term");
            fileWriter.append(';');
            fileWriter.append("Date");
            fileWriter.append('\n');
            while (rs.next()) {
                fileWriter.append(rs.getString(1));
                fileWriter.append(';');
                fileWriter.append(rs.getString(2));
                fileWriter.append(';');
                fileWriter.append(rs.getString(3));
                fileWriter.append(';');
                fileWriter.append(rs.getString(4));
                fileWriter.append(';');
                fileWriter.append(rs.getString(5));
                fileWriter.append(';');
                fileWriter.append(rs.getString(6));
                fileWriter.append(';');
                fileWriter.append('\n');
            }
            fileWriter.flush();

            System.out.println("==================================================================");
            System.out.println("SUCCESS: PositiveImpressions CSV File was created successfully.");
            System.out.println("==================================================================");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

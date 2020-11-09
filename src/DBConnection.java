import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;




public class DBConnection {
    /**
     * Connect to the test.db database
     * @return the Connection object
     */

    private final String url = "jdbc:sqlite:database/ImpresionMiner.db";

//Only one object of that class can be created.
/*******************************************************************/
/****************    Singleton Design Pattern    *******************/
/*******************************************************************/
    private static DBConnection instance = null;

    // private constructor
    private DBConnection()
    {

    }

    //synchronized method to control simultaneous access
    public synchronized static DBConnection getInstance()
    {
        if (instance == null)
        {
            // if instance is null, initialize
            instance = new DBConnection();
        }
        return instance; //If object already exists, return a reference.
    }
/******************************************************************/
/******************************************************************/

    //Print to console all URLs from the urls table.
    public void printAllUrls(){
        String sql = "SELECT id, url FROM urls";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t" +
                        rs.getString("url"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    //Insert new record into urls table.
    public int insertUrl(String url) {
        String sql = "INSERT INTO urls(url) VALUES(?)";
        int rowsAffected;
        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, url);
            rowsAffected = pstmt.executeUpdate();
            return rowsAffected;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }
    }

    //Insert new record into negativeterms table.
    public void insertNegativeTerm(String term) {
        String sql = "INSERT INTO negativeterms(term) VALUES(?)";
        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, term);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Insert new record into positiveterms table.
    public void insertPositiveTerm(String term) {
        String sql = "INSERT INTO positiveterms(term) VALUES(?)";
        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, term);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    //View Keyword Searches in URLs and Negative-Positive impressions
    public void selectAllResults(){
        String sql = "SELECT k.id, u.url, k.keyword, k.shown, k.timestamp FROM keywords AS k INNER JOIN urls AS u ON k.url_id = u.id;";
        String sql2 = "SELECT t.term FROM negativeimpressions AS i INNER JOIN negativeterms AS t ON i.term_id = t.id WHERE i.keywords_id = ? ;";
        String sql3 = "SELECT t.term FROM positiveimpressions AS i INNER JOIN positiveterms AS t ON i.term_id = t.id WHERE i.keywords_id = ? ;";

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt  = conn.prepareStatement(sql);
            PreparedStatement pstmt2 = conn.prepareStatement(sql2);
            PreparedStatement pstmt3 = conn.prepareStatement(sql3);
            ResultSet rs    = pstmt.executeQuery()) {

            int keywordsId;

            // loop through the result set
            while (rs.next()) {
                keywordsId = rs.getInt(1);  //Store the id of the record from the keywords table.
                System.out.println("ID: "+ keywordsId +" ,URL: " + rs.getString(2) + " ,Keyword: " +  rs.getString(3) +
                        " ,Times shown: " + rs.getInt(4) +" ,Timestamp: " + rs.getString(5) );

                pstmt2.setInt(1,keywordsId);

                try(ResultSet rs2 = pstmt2.executeQuery();) {
                    if (rs2 != null) {
                        System.out.print("Negative Terms: ");
                        while (rs2.next()) {
                            System.out.print(rs2.getString(1) + "\t");

                        }

                    } else {
                        System.out.println("No Negative terms found");
                    }
                    System.out.println();
                }
                pstmt3.setInt(1,keywordsId);

                try(ResultSet rs3 = pstmt3.executeQuery();) {
                    if (rs3 != null) {
                        System.out.print("Positive Terms: ");
                        while (rs3.next()) {
                            System.out.print(rs3.getString(1) + "\t");

                        }
//                        rs3.close();
                    } else {
                        System.out.println("No Positive terms found");
                    }

                    System.out.println();
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Print to console, keyword searches and negative-positive terms found , between the 2 dates given.
    public void selectAllResultsByDate(String firstDate,String secondDate){
        String sql = "SELECT k.id, u.url, k.keyword, k.shown, k.timestamp FROM keywords AS k INNER JOIN urls AS u ON k.url_id = u.id " +
                "WHERE date(k.timestamp) BETWEEN ? AND ? ;";
        String sql2 = "SELECT t.term FROM negativeimpressions AS i INNER JOIN negativeterms AS t ON i.term_id = t.id WHERE i.keywords_id = ? ;";
        String sql3 = "SELECT t.term FROM positiveimpressions AS i INNER JOIN positiveterms AS t ON i.term_id = t.id WHERE i.keywords_id = ? ;";


        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt  = conn.prepareStatement(sql);
            PreparedStatement pstmt2 = conn.prepareStatement(sql2);
            PreparedStatement pstmt3 = conn.prepareStatement(sql3)) {

            pstmt.setString(1,firstDate);
            pstmt.setString(2,secondDate);


            boolean hasRows = false;
            int keywordsId;

            // loop through the result set
            try(ResultSet rs  = pstmt.executeQuery();) {
                while (rs.next()) {
                    hasRows = true;

                    keywordsId = rs.getInt(1);  //field id from keywords table
                    System.out.println("ID: " + keywordsId + " ,URL: " + rs.getString(2) + " ,Keyword: " + rs.getString(3) +
                            " ,Times shown: " + rs.getInt(4) + " ,Timestamp: " + rs.getString(5));

                    pstmt2.setInt(1, keywordsId);    //Get the negative terms from negativeimpressions table for the given "keywords_id".

                    try(ResultSet rs2 = pstmt2.executeQuery();) {
                        if (rs2 != null) {
                            System.out.print("Negative Terms: ");
                            while (rs2.next()) {
                                System.out.print(rs2.getString(1) + "\t");

                            }
//                            rs2.close();
                        } else {
                            System.out.println("No Negative terms found");
                        }
                        System.out.println();
                    }
                    pstmt3.setInt(1, keywordsId);    //Get the negative terms from positiveimpressions table for the given "keywords_id".

                    try(ResultSet rs3 = pstmt3.executeQuery();) {
                        if (rs3 != null) {
                            System.out.print("Positive Terms: ");
                            while (rs3.next()) {
                                System.out.print(rs3.getString(1) + "\t");

                            }

                        } else {
                            System.out.println("No Positive terms found");
                        }

                        System.out.println();
                    }
                }
            }

            if(!hasRows)
                System.out.println("No Keywords were found between those dates.");


        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Print to console all negative impressions found on keyword searches. All records from negativeimpressions table.
    public void selectNegativeImpressions(){
        String sql = "SELECT id,url,keyword,term,timestamp FROM negativeimpressions";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            System.out.println("\nNegative Impressions in Keyword searches");
            System.out.println("========================================================================================================================================================");
            System.out.println("ID\t\t\t\t|\t\t\t\tURL\t\t\t\t|\t\t\t\tKeyword\t\t\t\t|\t\t\t\tTerm\t\t\t\t|\t\t\t\tDate");
            System.out.println("========================================================================================================================================================");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t\t\t\t" +
                        "\t\t" +rs.getString("url")    + "\t\t\t\t"	 +
                        "\t\t" +rs.getString("keyword")    + "\t\t\t\t" +
                        "\t\t" +rs.getString("term")         + "\t\t\t\t" +
                        "\t\t" +rs.getString("timestamp")    + "\t\t\t\t"

                );
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Print to console all positive impressions found on keyword searches. All records from positiveimpressions table.
    public void selectPositiveImpressions(){
        String sql = "SELECT id,url,keyword,term,timestamp FROM positiveimpressions";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            System.out.println("\nPositive Impressions in Keyword searches");
            System.out.println("========================================================================================================================================================");
            System.out.println("ID\t\t\t\t|\t\t\t\tURL\t\t\t\t|\t\t\t\tKeyword\t\t\t\t|\t\t\t\tTerm\t\t\t\t|\t\t\t\tDate");
            System.out.println("========================================================================================================================================================");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t\t\t\t" +
                        "\t\t" +rs.getString("url")    + "\t\t\t\t" +
                        "\t\t" +rs.getString("keyword")    + "\t\t\t\t" +
                        "\t\t" +rs.getString("term")         + "\t\t\t\t" +
                        "\t\t" +rs.getString("timestamp")    + "\t\t\t\t"

                );
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    //Print to console all negative terms stored in negativeterms table.
    public void selectNegativeTerms(){
        String sql = "SELECT id,term FROM negativeterms";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            System.out.println("\nNegative Terms");
            System.out.println("========================================================================================");
            System.out.println("ID\t\t\t\t|\t\t\t\tTerm\t\t\t\t");
            System.out.println("========================================================================================");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t\t\t\t" +
                        "\t\t" +rs.getString("term")    + "\t\t\t\t"

                );
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    //Print to console all negative terms stored in negativeterms table.
    public void selectPositiveTerms(){
        String sql = "SELECT id,term FROM positiveterms";

        try( Connection conn = DriverManager.getConnection(url);
             Statement stmt  = conn.createStatement();
             ResultSet rs    = stmt.executeQuery(sql)) {

            System.out.println("\nPositive Terms");
            System.out.println("========================================================================================");
            System.out.println("ID\t\t\t\t|\t\t\t\tTerm\t\t\t\t");
            System.out.println("========================================================================================");
            // loop through the result set
            while (rs.next()) {
                System.out.println(rs.getInt("id") +  "\t\t\t\t" +
                        "\t\t" +rs.getString("term")    + "\t\t\t\t"

                );
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

    }

    //Receives the id of the negative term as a parameter and returns the negative term.
    public String selectNegativeTermByID(int id){
        String sql = "SELECT term FROM negativeterms WHERE id=?";

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt  = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);

            // loop through the result set
            try(ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    String negativeterm = rs.getString("term");

                    return negativeterm;
                }
            }

            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    //Receives the id of the positive term as a parameter and returns the positive term.
    public String selectPositiveTermByID(int id){
        String sql = "SELECT term FROM positiveterms WHERE id=?";

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt  = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);

            // loop through the result set
            try(ResultSet rs = pstmt.executeQuery();) {
                while (rs.next()) {
                    String positiveterm = rs.getString("term");

                    return positiveterm;
                }
            }

            return null;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    //Stores a new record into keywords table.
    public int insertKeyword(Integer urlId,String keyword,int shown){
        String sql = "INSERT INTO keywords(url_id,keyword,shown,timestamp) VALUES(?,?,?,?)";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String ts = sdf.format(timestamp);

        /*lock1.lock();*/
        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql,Statement.RETURN_GENERATED_KEYS)) {

            pstmt.setInt(1, urlId);
            pstmt.setString(2, keyword);
            pstmt.setInt(3, shown);
            pstmt.setString(4, ts);

            int rowsAffected = pstmt.executeUpdate();

            if(rowsAffected > 0) {
                try(ResultSet rs = pstmt.getGeneratedKeys();) {
                    int id;
                    if (rs.next()) {
                        id = rs.getInt(1);

                        return id;
                    } else {
                        return -1;
                    }
                }
            }
            else{
                return -1;
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }


    }

    //Takes the id of the URL as a parameter and returns the URL.
    public String selectUrlbyID(int id){
        String sql = "SELECT url FROM urls WHERE id=?";

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);
            try(ResultSet rs = pstmt.executeQuery();) {

                String url;

                if (rs.next()) {
                    url = rs.getString("url");

                    return url;
                } else {

                    return null;
                }
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }

    }

    //Deletes the record in urls table, with the given id.
    public int deleteUrlbyID(int id){
        String sql = "DELETE FROM urls WHERE id=?";
        int rowsAffected;

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);
            rowsAffected = pstmt.executeUpdate();

            return rowsAffected;

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }

    }

    //Deletes the record from the negativeterms table with the given id.
    public int deleteNegativebyID(int id){
        String sql = "DELETE FROM negativeterms WHERE id=?";
        int rowsAffected;

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);
            rowsAffected = pstmt.executeUpdate();

            return rowsAffected;
            //rs.close();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }

    }

    //Deletes the record from the positiveterms table with the given id.
    public int deletePositivebyID(int id){
        String sql = "DELETE FROM positiveterms WHERE id=?";
        int rowsAffected;

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1,id);
            rowsAffected = pstmt.executeUpdate();

            return rowsAffected;


        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }

    }

    //Takes 2 parameters, the id of the record to be updated and the new URL of this record. Then updates this record in the urls table.
    public int editUrlbyID(int id,String URL){
        String sql = "UPDATE urls SET url=? WHERE id=?";
        int rowsAffected;

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1,URL);
            pstmt.setInt(2,id);

            rowsAffected = pstmt.executeUpdate();

            return rowsAffected;

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return -1;
        }

    }

    //Given the parameter "extStr" , splits the extStr string and searches for positive term matches in all the substrings.
    public int searchPositiveImpressions(Integer urlid,int keywordsId,String extStr){

        String impressionsArray[] = extStr.split(" ",10);   //Split the String into an array of strings seperated from blanks.The pattern " " will be applied at most limit-1 times.
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String ts = sdf.format(timestamp);  //Make timestamp

        //Queries
        String sql = "SELECT id,term FROM positiveterms ";
        String sql2 = "INSERT INTO positiveimpressions(url_id,keywords_id,term_id,timestamp) VALUES(?,?,?,?)";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql);
            PreparedStatement pstmt = conn.prepareStatement(sql2)) {

            // loop through the result set, which has stored all the positive terms, from positive terms database table.
            while (rs.next()) {
                for(String token : impressionsArray){   //Loop through every substring stored in the impressionArray table.
                    if(token.equalsIgnoreCase(rs.getString("term"))){   //If a value from the impressionsArray table(substrings from the original string) ,equals a positive term,
                                                                                    //then insert a new record into positiveimpressions database table.
                        pstmt.setInt(1, urlid);
                        pstmt.setInt(2, keywordsId);
                        pstmt.setInt(3, rs.getInt("id"));
                        pstmt.setString(4, ts);
                        pstmt.executeUpdate();

                        int positiveimp = rs.getInt("id");

                        return positiveimp;

                    }
                }

            }

            return -1;

        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }

    }

    //Given the parameter "extStr" , splits the extStr string and searches for negative term matches in all the substrings.
    public int searchNegativeImpressions(Integer urlid,int keywordsId,String extStr) {

            String impressionsArray[] = extStr.split(" ",10);   //Split the String into an array of strings seperated from blanks.
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            String ts = sdf.format(timestamp);  //Make timestamp

            String sql3 = "SELECT id,term FROM negativeterms ";
            String sql4 = "INSERT INTO negativeimpressions(url_id,keywords_id,term_id,timestamp) VALUES(?,?,?,?)";

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt2 = conn.createStatement();
            ResultSet rs2 = stmt2.executeQuery(sql3);
            PreparedStatement pstmt2 = conn.prepareStatement(sql4)) {

            //loop through the result set, which has stored all the negative terms, from positive terms database table.
            while (rs2.next()) {
            for (String token : impressionsArray) { //Loop through every substring stored in the impressionArray table.
                if (token.equalsIgnoreCase(rs2.getString("term"))) {    //If a value from the impressionsArray table(substrings from the original string) ,equals a positive term,
                                                                                    //then insert a new record into positiveimpressions database table.

                    pstmt2.setInt(1, urlid);
                    pstmt2.setInt(2, keywordsId);
                    pstmt2.setInt(3, rs2.getInt("id"));
                    pstmt2.setString(4, ts);
                    pstmt2.executeUpdate();
                    int negativeimp = rs2.getInt("id");

                    return negativeimp;
                }
            }

            }

            return -1;
        }
        catch(SQLException e){
            e.printStackTrace();
            return -1;
        }
    }

    //The method receives parameter "count", which is the number a keyword was found in an URL and "id" which is the id of the record.
    //Then updates the shown field with the "count" parameter in the record given by "id" parameter, of the keywords table.
    public void updateKeywordsCountById(int count,int id)  {
        String sql = "UPDATE keywords SET shown=? WHERE id=?";

        try(Connection conn = DriverManager.getConnection(url);
            PreparedStatement pstmt = conn.prepareStatement(sql);) {

            pstmt.setInt(1,count);
            pstmt.setInt(2,id);
            pstmt.executeUpdate();

        }
        catch(SQLException e){
            e.printStackTrace();
        }
    }

    //Returns all URLs from the urls table.
    public ArrayList<String> selectAllUrls(){

        ArrayList<String> urls = new ArrayList<>();
        String sql = "SELECT url FROM urls";
        int i=0;

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            while (rs.next()) {
                urls.add(rs.getString("url"));

            }

            return urls;
        }
     catch (SQLException e) {
        System.out.println(e.getMessage());
        return null;
    }

    }

    //Returns all IDs from urls table.
    public ArrayList<Integer> selectAllUrlsId() {
        ArrayList<Integer> urlsIds = new ArrayList<>();
        String sql = "SELECT id FROM urls";
        int i=0;

        try(Connection conn = DriverManager.getConnection(url);
            Statement stmt  = conn.createStatement();
            ResultSet rs    = stmt.executeQuery(sql)) {

            while (rs.next()) {
                urlsIds.add(rs.getInt("id"));

            }

            return urlsIds;
        }
        catch (SQLException e) {
            System.out.println(e.getMessage());
            return null;
        }

    }


}


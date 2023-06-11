import java.sql.*;
import java.util.List;

public class JDBC {
    private static String dburl= "";
    private static String dbUser = "";
    private static String dbPw = "";
    private static Connection conn;
    public JDBC(){
        setConnection();
    }
    public void setConnection(){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(dburl, dbUser, dbPw);
        }catch(SQLException | ClassNotFoundException e){
            e.printStackTrace();
        }

    }
    public void closeConnection() throws SQLException {
        conn.close();
    }
    public void createTable(String name, List columns) throws SQLException{
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        String sql = sb.append("CREATE TABLE "+name+" ("+String.join(",",columns)+")").toString();
        stmt.execute(sql);
    }
    public void createIndex(String name, String column) throws SQLException {
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        String sql = sb.append("CREATE INDEX "+column+" ON "+name+" ("+column+")").toString();
        stmt.execute(sql);
    }
    public void saveData(String name, List column, String value) throws SQLException {
        String sql = "INSERT INTO "+name+"("+String.join(",",column)+") values (?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(sql);
        List<String> tmp = List.of(value.split(","));
        for(int i=0; i<10; i++){
            pstmt.setString(i+1, tmp.get(i));
        }
        pstmt.executeUpdate();
    }
    public ResultSet getSpecificDataByIndexing(String name,String value) throws SQLException{
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        String sql = sb.append("SELECT * FROM "+name+" WHERE no = "+value).toString();
        ResultSet resultSet = stmt.executeQuery(sql);
        return resultSet;
    }

    public ResultSet getSingleColumnData(String name, String column) throws SQLException {
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        StringBuilder sb = new StringBuilder();
        String sql = sb.append("SELECT "+column+" FROM "+name).toString();
        ResultSet resultSet = stmt.executeQuery(sql);
        return resultSet;
    }

}

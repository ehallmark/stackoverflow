package database;

import com.google.gson.Gson;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Database {
    private static Connection conn;
    static {
        try {
            conn = DriverManager.getConnection("jdbc:postgresql://localhost/stackoverflow?user=postgres&password=password&tcpKeepAlive=true");
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static List<Map<String,Object>> loadData(String table, String... fields) throws SQLException {
        List<Map<String,Object>> data = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement("select "+String.join(",",fields)+" from "+table);
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            Map<String,Object> row = new HashMap<>();
            for(int i = 0; i < fields.length; i++) {
                row.put(fields[i], rs.getObject(i+1));
            }
            data.add(row);
        }
        rs.close();
        ps.close();
        return data;
    }

    public static String selectAnswerBody(String answerId)  {
        try {
            PreparedStatement ps = conn.prepareStatement("select body from posts where id=?");
            ps.setInt(1, Integer.valueOf(answerId));
            ResultSet rs = ps.executeQuery();
            String ret = null;
            if (rs.next()) {
                ret = rs.getString(1);
            }
            rs.close();
            ps.close();
            return ret;
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Integer selectParentIdOf(String answerId)  {
        try {
            PreparedStatement ps = conn.prepareStatement("select parent_id from posts where id=?");
            ps.setInt(1, Integer.valueOf(answerId));
            ResultSet rs = ps.executeQuery();
            Integer ret = null;
            if (rs.next()) {
                ret = rs.getInt(1);
            }
            rs.close();
            ps.close();
            return ret;
        } catch(Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static Map<Integer,List<Object>> loadMap(String table, String idField, String valueField) throws SQLException {
        Map<Integer,List<Object>> data = new HashMap<>();
        PreparedStatement ps = conn.prepareStatement("select "+idField+","+valueField+" from "+table);
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            Integer id = rs.getInt(1);
            data.putIfAbsent(id, new ArrayList<>());
            data.get(id).add(rs.getObject(2));
        }
        rs.close();
        ps.close();
        return data;
    }


    public static Map<Integer,Map<String,Double>> loadMapWithValue(String table, String idField, String valueField, String numericField) throws SQLException {
        Map<Integer,Map<String,Double>> data = new HashMap<>();
        PreparedStatement ps = conn.prepareStatement("select "+idField+","+valueField+","+numericField+" from "+table);
        ps.setFetchSize(100);
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            Integer id = rs.getInt(1);
            data.putIfAbsent(id, new HashMap<>());
            data.get(id).put(rs.getString(2), rs.getDouble(3));
        }
        rs.close();
        ps.close();
        return data;
    }

    public static List<String> loadSingleColumn(String columnName, String tableName) throws SQLException {
        List<String> data = new ArrayList<>();
        PreparedStatement ps = conn.prepareStatement("select "+columnName+" from "+tableName);
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            data.add(rs.getString(1));
        }
        rs.close();
        ps.close();
        return data;
    }

}

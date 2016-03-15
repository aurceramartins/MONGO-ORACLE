package exa17oraclemongo;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import java.io.FileNotFoundException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.bson.Document;

public class Exa17oraclemongo {

    public static Connection conexion = null;
    public static MongoCollection<Document> vendas;

    public static Connection getConexion() throws SQLException {
        String usuario = "hr";
        String password = "hr";
        String host = "localhost"; // tambien puede ser una ip como "192.168.1.14"
        String puerto = "1521";
        String sid = "orcl";
        String ulrjdbc = "jdbc:oracle:thin:" + usuario + "/" + password + "@" + host + ":" + puerto + ":" + sid;

        conexion = DriverManager.getConnection(ulrjdbc);
        return conexion;
    }

    public static void closeConexion() throws SQLException {
        conexion.close();
    }

    public void cambiarStock(String codigop, double cantidade) throws SQLException {
        conexion.createStatement().executeUpdate("update produtos set stock= stock - " + cantidade + " where codigop ='" + codigop + "'");
    }

    public void aumentarGasto(String codcli, String codpro, double cantidade) throws SQLException {
        String laConsulta = "SELECT * FROM produtos";
        Statement stmtConsulta = conexion.createStatement();
        ResultSet rs = conexion.createStatement().executeQuery("select prezo from produtos where codigop='" + codpro + "'");
        while (rs.next()) {
            double total = cantidade * rs.getInt(1);
            conexion.createStatement().executeUpdate("update clientes set gasto= gasto+" + total + "where codigoc='" + codcli + "'");
        }
    }

    public void inserirVendas(String codcli, String codpro, String data, double cantidade) throws SQLException {
        Statement stmtConsulta = conexion.createStatement();
        ResultSet rs = conexion.createStatement().executeQuery("select prezo from produtos where codigop='" + codpro + "'");
        while (rs.next()) {
            double total = cantidade * rs.getInt(1);
            conexion.createStatement().executeUpdate("insert into vendas values('" + codcli + "','" + codpro + "','" + data + "'," + total + ")");
        }
    }

    public void inserirVendasMongo(String codcli, String codpro, String data, double cantidade) throws SQLException {
        vendas.insertOne(new Document("codcli", codcli).append("codpro", codpro).append("catidade", cantidade).append("data", data));

    }

    public static void main(String[] args) throws FileNotFoundException, SQLException {
        Exa17oraclemongo exa = new Exa17oraclemongo();
        Exa17oraclemongo.getConexion();
//conexion host
        MongoClient cliente = new MongoClient("localhost", 27017);
        //conexion base
        //MongoDatabase base = cliente.getDatabase("cidades");
        MongoDatabase base = cliente.getDatabase("tenda");
        //conexion colecion
        //MongoCollection<Document> colecion = base.getCollection("persoas");
        MongoCollection<Document> colecion = base.getCollection("pedidos");
        vendas = base.getCollection("vendas");

        FindIterable<Document> cursor = colecion.find();
        MongoCursor<Document> iterator1 = cursor.iterator();

        while (iterator1.hasNext()) {
            Document docu = iterator1.next();
            System.out.println(docu);
            Double cantidade = docu.getDouble("cantidade");
            String codpro = docu.getString("codpro");
            String codcli = docu.getString("codcli");
            String data = docu.getString("data");

            exa.cambiarStock(codpro, cantidade);
            exa.aumentarGasto(codcli, codpro, cantidade);
            exa.inserirVendas(codcli, codpro, data, cantidade);
            exa.inserirVendasMongo(codcli, codpro, data, cantidade);
        }

        closeConexion();
    }
}

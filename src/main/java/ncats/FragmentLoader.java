package ncats;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;

/*
sbt "run-main ncats.FragmentLoader probedb.props NCGC part-00009-efc33fe8-f37d-48d9-85d9-83da110a1bc2-c000.csv"
*/

public class FragmentLoader implements AutoCloseable {
    static final Logger logger =
        Logger.getLogger(FragmentLoader.class.getName());
    
    final Connection con;
    final PreparedStatement pstm;
    
    public FragmentLoader (String jdbcUrl, String username, String password)
        throws SQLException {
        con = DriverManager.getConnection(jdbcUrl, username, password);
        con.setAutoCommit(false);
        pstm = con.prepareStatement
            ("insert into lychi_fragment(id,lychi_h1,lychi_h2,lychi_h3,"
             +"lychi_h4,lychi_h5,fragment_h4,fragment_smiles,source,"
             +"created,batch_file) values(?,?,?,?,?,?,?,?,?,?,?)");
    }

    public void close () throws Exception {
        con.close();
    }

    public int load (String source, String batch, InputStream is) 
        throws Exception {
        int rows = 0;
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader (is))) {
            String[] header = null;
            for (String line; (line = br.readLine()) != null; ) {
                String[] toks = line.split("\t");
                if (line.startsWith("STRUC_ID")) {
                    header = toks;
                }
                else {
                    pstm.setString(1, toks[0]); //id
                    if (toks.length > 1) {
                        pstm.setString(2, toks[1]); //h1
                        pstm.setString(3, toks[2]); //h2
                        pstm.setString(4, toks[3]); //h3
                        pstm.setString(5, toks[4]); //h4
                        pstm.setString(6, toks[5]); //h5
                        if (toks.length > 6) {
                            pstm.setString(7, toks[6]); //fragment_h4
                            pstm.setString(8, toks[7]); //fragment_smiles
                        }
                        else {
                            pstm.setString(7, null);
                            pstm.setString(8, null);
                        }
                    }
                    else {
                        for (int i = 2; i < 9; ++i)
                            pstm.setString(i, null);
                    }
                    pstm.setString(9, source);
                    pstm.setTimestamp(10, new Timestamp
                                      (System.currentTimeMillis()));
                    pstm.setString(11, batch);
                    if (pstm.executeUpdate() > 0) {
                        ++rows;
                        if (rows % 1000 == 0) {
                            logger.info("==== "+rows+": "+toks[0]);
                            con.commit();
                        }
                    }
                }
            }
            con.commit();
            logger.info("==== "+batch+": "+rows+" row(s) inserted!");
        }
        return rows;
    }

    public int load (String source, File... files) throws Exception {
        int rows = 0;
        for (File f : files) {
            int r = load (source, f.getName(), new FileInputStream (f));
            rows += r;
        }
        return rows;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            logger.info("Usage: "+FragmentLoader.class.getName()
                        +" DB.props SOURCE FILES...");
            System.exit(1);
        }

        File f = new File (argv[0]);
        Properties props = new Properties ();
        props.load(new FileReader (f));

        f = new File (argv[1]);
        if (f.exists()) {
            logger.log(Level.SEVERE,
                       argv[1]+": doesn't look like a source name!");
            System.exit(1);
        }
        String source = argv[1];

        List<File> files = new ArrayList<>();
        for (int i = 2; i < argv.length; ++i) {
            f = new File (argv[i]);
            if (f.exists())
                files.add(f);
            else
                logger.warning(f+": file not exist!");
        }

        try (FragmentLoader loader = new FragmentLoader
             (props.getProperty("url"), props.getProperty("username"),
              props.getProperty("password"))) {
            int rows = loader.load(source, files.toArray(new File[0]));
            logger.info("========= "+rows+" total row(s) inserted!");
        }
    }
}

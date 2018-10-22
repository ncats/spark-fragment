package ncats;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.net.URL;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.zip.GZIPInputStream;
import java.util.function.Consumer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.spark.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.types.MapType;
import scala.collection.JavaConverters;
//import scala.collection.immutable.Map;
import scala.Predef;
import scala.Tuple2;

import chemaxon.util.MolHandler;
import chemaxon.struc.Molecule;
import tripod.chem.MolecularFramework;
import lychi.LyChIStandardizer;
import lychi.util.ChemUtil;

public class SparkFragments implements AutoCloseable {
    static final Logger logger =
        Logger.getLogger(SparkFragments.class.getName());

    public static class GenerateFragments implements Function<Row, Row> {

        transient MolHandler mh;
        transient MolecularFramework mf;
        transient LyChIStandardizer lychi;

        public GenerateFragments () {
        }

        protected Molecule getMolecule (String smiles) throws Exception {
            if (mh == null) {
                mh = new MolHandler ();
            }
            mh.setMolecule(smiles);
            return mh.getMolecule();
        }

        protected Molecule standardize (Molecule mol) throws Exception {
            if (lychi == null) {
                lychi = new LyChIStandardizer ();
            }
            lychi.standardize(mol);
            return mol;
        }

        protected Enumeration<Molecule> generateFragments (Molecule mol)
            throws Exception {
            if (mf == null) {
                mf = new MolecularFramework ();
                mf.setGenerateAtomMapping(true);
                //mf.setKeepFusedRings(true);
                mf.setAllowBenzene(false);
            }
            mf.setMolecule(mol);
            mf.run();
            return mf.getFragments();
        }

        public Row call (Row row) {
            String struc = row.getString(0); // STRUCTURE
            String id = row.getString(1); // STRUC_ID
            try {
                Molecule mol = getMolecule (struc);
                String[] hk = LyChIStandardizer.hashKeyArray(mol);
                String l5 = hk[hk.length-1];

                try {
                    standardize (mol);
                    hk = LyChIStandardizer.hashKeyArray(mol);
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't standardize "+id, ex);
                }
                
                java.util.Map<String, String> frags = new HashMap<>();
                for (Enumeration<Molecule> en = generateFragments (mol);
                     en.hasMoreElements();) {
                    Molecule f = en.nextElement();
                    frags.put(f.getName(), ChemUtil.canonicalSMILES(f));
                }

                Object fmap = null;
                if (frags.isEmpty()) {
                    logger.log(Level.WARNING, "** Structure "+id
                               +" has no fragments! **");
                    /*
                      functions.lit(null).cast
                      (MapType.apply(DataTypes.StringType,
                                       DataTypes.StringType));
                    */
                }
                else 
                    fmap = toScalaMap (frags);
                
                return RowFactory.create
                    (id, hk[0], hk[1], hk[2], hk[3], l5, fmap);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE,
                           "Can't generate fragments for "+id, ex);
                ex.printStackTrace();
            }
            return RowFactory.create(id, null, null, null, null, null, null);
        }

        /*
        public static <K, V> scala.collection.immutable.Map<K, V>
            toScalaImmutableMap (java.util.Map<K, V> jmap) {
            List<Tuple2<K, V>> tuples = jmap.entrySet()
                .stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());
            
            return (Map<K, V>) Map$.MODULE$
                .apply(JavaConverters.asScalaBuffer(tuples).toSeq());
        }
        */

        public static <A, B> scala.collection.immutable.Map<A, B>
            toScalaMap (java.util.Map<A, B> m) {
            return JavaConverters.mapAsScalaMapConverter(m)
                .asScala().toMap(Predef.<Tuple2<A, B>>conforms());
        }
    }

    public static class QueryProbeDbSamples
        implements Function<String, Row>, AutoCloseable {
        final String jdbcUrl;
        final String username;
        final String password;

        transient Connection con;
        transient PreparedStatement pstm;

        public QueryProbeDbSamples (String jdbcUrl,
                                    String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        Connection getConnection () throws SQLException {
            if (con == null)
                con = DriverManager.getConnection(jdbcUrl, username, password);
            return con;
        }

        ResultSet getSample (String id) throws SQLException {
            if (pstm == null) {
                pstm = getConnection().prepareStatement
                 ("select smiles_iso "
                  +"from ncgc_sample where sample_id = ? "
                  +"and smiles_iso is not null");
            }
            pstm.setString(1, id);
            return pstm.executeQuery();
        }

        public Row call (String id) {
            try (ResultSet rs = getSample (id)) {
                if (rs.next()) {
                    String smiles = rs.getString("smiles_iso");
                    return RowFactory.create(smiles, id);
                }
                else {
                    logger.log(Level.WARNING, "Can't lookup sample \""+id+"\"");
                }
            }
            catch (SQLException ex) {
                logger.log(Level.SEVERE, "Can't execute query for "+id, ex);
                ex.printStackTrace();
            }
            
            return RowFactory.create(null, id);
        }

        public void close () throws Exception {
            if (con != null)
                con.close();
        }
    }
    
    final String jdbcUrl;
    final String username;
    final String password;
    final SparkSession spark;

    public SparkFragments (String jdbcUrl, String username, String password)
        throws Exception {
        if (jdbcUrl == null)
            throw new IllegalArgumentException ("No Jdbc URL specified!");

        logger.info("### JDBC URL: "+jdbcUrl);
        
        spark = SparkSession
            .builder()
            //.master("local[*]")
            //.config("spark.driver.bindAddress", "127.0.0.1")
            //.config("spark.driver.host", "127.0.0.1")
            .appName(SparkFragments.class.getName())
            .getOrCreate();
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void close () throws Exception {
        spark.stop();
    }

    public Dataset<Row> loadProbeDb () throws Exception {
        return probeDbSQL ("(select smiles_iso as STRUCTURE, "
                           +"sample_id as STRUC_ID, "
                           +"rownum as partcol from ncgc_sample "
                           +"where smiles_iso is not null"
                           //+" and rownum <= 1000"
                           +")"
                           );
    }

    public Dataset<Row> loadChEMBL () throws Exception {
        return chemblSQL ("(select a.molfile as STRUCTURE, "
                          +"b.chembl_id as STRUC_ID, "
                          +"a.molregno from compound_structures a, "
                          +"chembl_id_lookup b where "
                          +"a.molregno = b.entity_id and "
                          +"b.entity_type='COMPOUND'"
                          //+" limit 1000"
                          +") as chembl");
    }

    public Dataset<Row> probeDbSQL (String sql) throws Exception {
        return spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", sql)
            .option("user", username)
            .option("password", password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            .option("numPartitions", 10)
            .option("fetchsize", 1000)
            .option("partitionColumn", "partcol")
            .option("lowerBound", 0)
            .option("upperBound", 1000000)
            .load();
    }

    public Dataset<Row> chemblSQL (String sql) throws Exception {
        return spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", sql)
            .option("user", username)
            .option("password", password)
            .option("driver", "com.mysql.jdbc.Driver")
            .option("numPartitions", 10)
            .option("fetchsize", 1)
            .option("partitionColumn", "molregno")
            .option("lowerBound", 0)
            .option("upperBound", 2200000)
            .load();
    }

    public Dataset<Row> probeDbFile (String file) throws Exception {
        try (QueryProbeDbSamples qmap = new QueryProbeDbSamples
             (jdbcUrl, username, password)) {
            StructType schema = new StructType ()
                .add("STRUCTURE", DataTypes.StringType)
                .add("STRUC_ID", DataTypes.StringType, false)
                ;
            JavaSparkContext jsc = new JavaSparkContext (spark.sparkContext());
            return spark.createDataFrame
                (jsc.textFile(file).map(qmap), schema);
        }
    }

    public Dataset<Row> generateFragments (Dataset<Row> df, String output)
        throws Exception {
        JavaRDD<Row> rdd = df.select("STRUCTURE", "STRUC_ID").javaRDD();
        logger.info("### enumerating fragments for "+rdd.count()
                    +" structures; numParitions = "+rdd.getNumPartitions()
                    +" output = "+output);
        
        StructType schema = new StructType()
            .add("STRUC_ID", DataTypes.StringType, false)
            .add("LyChI_H1", DataTypes.StringType)
            .add("LyChI_H2", DataTypes.StringType)
            .add("LyChI_H3", DataTypes.StringType)
            .add("LyChI_H4", DataTypes.StringType)
            .add("LyChI_H5", DataTypes.StringType)
            .add("FRAGMENTS", new MapType (DataTypes.StringType,
                                           DataTypes.StringType, false));
        df = spark.createDataFrame(rdd.map(new GenerateFragments()), schema);
        df.show();
        
        df = df.select(df.col("STRUC_ID"),
                       df.col("LyChI_H1"),
                       df.col("LyChI_H2"),
                       df.col("LyChI_H3"),
                       df.col("LyChI_H4"),
                       df.col("LyChI_H5"),
                       functions.explode_outer(df.col("FRAGMENTS"))
                       .as(new String[]{"FRAGMENT_H4", "FRAGMENT_SMILES"}));
        df.show();

        df//.coalesce(1)
            .write()
            .mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("delimiter", "\t")
            .option("header", "true")
            .save(output);
        
        logger.info("### number of rows: "+df.count());
        return df;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+SparkFragments.class.getName()
                        +" [probedb|chembl].props [SAMPLE_FILE]");
            logger.info("where DB.props is a standard Java property format "
                        +"with the following properties defined for probedb "
                        +"or chembl database connection: username, password, "
                        +"url");
            System.exit(1);
        }

        Properties props = new Properties ();
        File dbconf = new File (argv[0]);
        props.load(new FileReader (dbconf));

        String fname = dbconf.getName().toLowerCase();
        try (SparkFragments spark =
             new SparkFragments (props.getProperty("url"),
                                 props.getProperty("username"),
                                 props.getProperty("password"))) {
            Dataset<Row> df;
            if (argv.length > 1) {
                df = spark.probeDbFile(argv[1]);
                df.show();
            }
            else {
                df = fname.startsWith("chembl") 
                    ? spark.loadChEMBL() : spark.loadProbeDb();
                /*
                df.write()
                    .mode(SaveMode.Overwrite)
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .save("dump");
                */
                df.printSchema();
            }
            spark.generateFragments(df, "fragments");
        }
    }
}

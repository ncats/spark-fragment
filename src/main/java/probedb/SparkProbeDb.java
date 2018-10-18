package probedb;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.net.URL;
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

public class SparkProbeDb implements AutoCloseable {
    static final Logger logger = Logger.getLogger(SparkProbeDb.class.getName());

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
            String smiles = row.getString(0);
            String id = row.getString(1);
            try {
                Molecule mol = getMolecule (smiles);
                String[] hk = LyChIStandardizer.hashKeyArray(mol);
                String l5 = hk[hk.length-1];
                
                standardize (mol);
                hk = LyChIStandardizer.hashKeyArray(mol);
                
                java.util.Map<String, String> frags = new HashMap<>();
                for (Enumeration<Molecule> en = generateFragments (mol);
                     en.hasMoreElements();) {
                    Molecule f = en.nextElement();
                    frags.put(f.getName(), ChemUtil.canonicalSMILES(f));
                }
                
                return RowFactory.create
                    (id, hk[0], hk[1], hk[2], hk[3], l5, toScalaMap (frags));
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
    
    final String jdbcUrl;
    final String username;
    final String password;
    final SparkSession spark;

    public SparkProbeDb (String jdbcUrl, String username, String password)
        throws Exception {
        if (jdbcUrl == null)
            throw new IllegalArgumentException ("No Jdbc URL specified!");

        logger.info("### JDBC URL: "+jdbcUrl);
        
        spark = SparkSession
            .builder()
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .appName(SparkProbeDb.class.getName())
            .getOrCreate();
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    public void close () throws Exception {
        spark.stop();
    }

    public Dataset<Row> registry () throws Exception {
        return load ("ncgc_sample");
    }
    
    public Dataset<Row> load (String sql) throws Exception {
        return spark.read()
            .format("jdbc")
            .option("url", jdbcUrl)
            .option("dbtable", sql)
            .option("user", username)
            .option("password", password)
            .option("driver", "oracle.jdbc.driver.OracleDriver")
            //.option("numPartitions", 10)
            .load();
    }

    public Dataset<Row> generateFragments (Dataset<Row> df, String output)
        throws Exception {
        StructType schema = new StructType()
            .add("SAMPLE_ID", DataTypes.StringType)
            .add("LyChI_H1", DataTypes.StringType)
            .add("LyChI_H2", DataTypes.StringType)
            .add("LyChI_H3", DataTypes.StringType)
            .add("LyChI_H4", DataTypes.StringType)
            .add("LyChI_H5", DataTypes.StringType)
            .add("FRAGMENTS", new MapType (DataTypes.StringType,
                                           DataTypes.StringType, false));
        
        df = spark.createDataFrame
            (df.select("SMILES_ISO", "SAMPLE_ID","SUPPLIER_ID")
             .javaRDD().map(new GenerateFragments()), schema);
        df.show();
        
        df = df.select(df.col("SAMPLE_ID"),
                       df.col("LyChI_H1"),
                       df.col("LyChI_H2"),
                       df.col("LyChI_H3"),
                       df.col("LyChI_H4"),
                       df.col("LyChI_H5"),
                       functions.explode(df.col("FRAGMENTS"))
                       .as(new String[]{"FRAGMENT_H4", "FRAGMENT_SMILES"}));
        df.show();

        df//.coalesce(1)
            .write()
            .mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("delimiter", "\t")
            .save(output);
        
        logger.info("### number of rows: "+df.count());
        return df;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+SparkProbeDb.class.getName()+" DB.props");
            logger.info("where DB.props is a standard Java property format "
                        +"with the following properties defined for probedb "
                        +"database connection: username, password, url");
            System.exit(1);
        }

        Properties props = new Properties ();
        props.load(new FileReader (argv[0]));
            
        try (SparkProbeDb probedb =
             new SparkProbeDb (props.getProperty("url"),
                               props.getProperty("username"),
                               props.getProperty("password"))) {
            Dataset<Row> df = probedb
                .load("(select * from ncgc_sample "
                      +"where smiles_iso is not null"
                      //+" and rownum <= 1000"
                      +")"
                      )
                ;
            df.printSchema();
            probedb.generateFragments(df, "fragments");
        }
    }
}

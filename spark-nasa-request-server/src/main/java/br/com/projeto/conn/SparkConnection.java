package br.com.projeto.conn;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Conexão com SparkContext.
 * 
 * @author Caio
 * @since 22/04/2018
 * @version 1.0
 */
public class SparkConnection {
	
	private JavaSparkContext spContext;
	
	private static final SparkConnection instance = new SparkConnection();
	
	public static SparkConnection getInstance(){
		return instance;
	}
	
	public JavaSparkContext getConnection(){
		
		String appName      = "Caio Semantix";
		String sparkMaster  = "local[2]";
			
		if (this.spContext == null){	
		
			SparkConf conf = new SparkConf().setAppName(appName).setMaster(sparkMaster);
			
			this.spContext = new JavaSparkContext(conf);
			
			return spContext;			 
		}
		else
			return spContext;
	}
}
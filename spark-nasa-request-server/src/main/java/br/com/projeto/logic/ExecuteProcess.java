package br.com.projeto.logic;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaSparkContext;

import br.com.projeto.conn.SparkConnection;
import scala.Tuple2;

/**
 * Classes responsável pela execução das operações.
 * 
 * @author Caio
 * @since 22/04/2018
 * @version 1.0
 */
public class ExecuteProcess {

	public static void main(String[] args) 
	throws IOException{
	
		final JavaSparkContext context = SparkConnection.getInstance().getConnection();
		 
		Stream<String> fileStreams = Stream.of("data/access_log_Jul95", "data/access_log_Aug95");
		
		fileStreams.forEach(file -> {
			
			ProcessAccessLog processAccessLog = new ProcessAccessLog(context, file);
			
			//Chamada logica de hosts
			long hosts 	= processAccessLog.getHostNumbers();
			
			//chamada logica de total 404
			long total404 = processAccessLog.getTotalFrom404();
			
			//chamada logica urils com 404
			List<Tuple2<String, Integer>> urls404Code 	= processAccessLog.get5Urlswith404Code();
			
			//404 por dia
			List<Tuple2<String, Integer>> urls404ToDay 	= processAccessLog.getQuant404toDay();
			
			//Total de Bytes do Dataset
			long totalBytes = processAccessLog.getTotalBytes();
					
			//Exibição da logica
			processAccessLog.showResult(hosts, total404, urls404Code, urls404ToDay, totalBytes);
		});
	}
}

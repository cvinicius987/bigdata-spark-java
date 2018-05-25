package br.com.projeto.logic;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.SizeEstimator;

import scala.Tuple2;

/**
 * Operações executadas sobre o arquivo de log
 * 
 * @author Caio
 * @since 22/04/2018
 * @version 1.0
 */
public class ProcessAccessLog {
	
	private String pathFile;
	private JavaRDD<String[]> arrFile;
			
	public ProcessAccessLog(JavaSparkContext context, String pathFile) {
		
		JavaRDD<String> file = context.textFile(pathFile).cache();
		
		this.pathFile = pathFile;
		this.arrFile  = file.map(str -> str.split("\\s"))
						    .filter(arr -> arr.length >= 10);
	}
	
	/**
	 * Retorna o total de hosts unicos do arquivo
	 * 
	 * @return long
	 */
	public long getHostNumbers(){
		
		return arrFile.map(arr -> arr[0])
					  .distinct()
					  .count();
	}	
	
	/**
	 * Retorna o total de erros 404
	 *
	 * @return long
	 */
	public long getTotalFrom404(){
		
		return arrFile.map(arr -> arr[8])
				      .filter(str -> str.equals("404"))
				      .count();
	}
	
	/**
	 * 3 URLs que mais causaram erro 404 
	 * 
	 * @return List<Tuple2<String, Integer>>
	 */
	public List<Tuple2<String, Integer>> get5Urlswith404Code() {
		
		Comparator<Tuple2<String, Integer>> tupleComparator = Comparator.comparing(tuple2 -> tuple2._2);
		
		return arrFile.filter(arr -> arr[8].equals("404"))
					   .mapToPair(arr -> new Tuple2<>(arr[0], 1))
					   .reduceByKey(Integer::sum)
					   .collect()
					   .stream()
					   .sorted(tupleComparator.reversed())
					   .limit(5)
					   .collect(Collectors.toList());
	}
	
	/**
	 * 4 Quantidade de erros 404 por dia
	 * 
	 * @return List<Tuple2<String, Integer>>
	 */
	public List<Tuple2<String, Integer>> getQuant404toDay() {
		
		return arrFile.filter(arr -> arr[8].equals("404"))
					   .mapToPair(arr -> new Tuple2<>(arr[3].substring(1, 12), 1))
					   .reduceByKey(Integer::sum)
					   .sortByKey()
					   .collect();
	}
	
	/**
	 * 5 Total de Bytes retornados
	 * 
	 * @return long
	 */
	public long getTotalBytes() {
		return SizeEstimator.estimate(arrFile);
	}
	
	/**
	 * Exibição dos resultados do processamento
	 * 
	 * @param hosts
	 * @param total404
	 * @param urls404Code
	 * @param urls404ToDay
	 * @param totalBytes
	 */
	public void showResult(long hosts,
						   long total404, 
						   List<Tuple2<String, Integer>> urls404Code, 
						   List<Tuple2<String, Integer>> urls404ToDay, 
						   long totalBytes) {
		
		System.out.println(String.format(" ==== Logica do arquivo %s", this.pathFile));
		
		System.out.println("\n1-) Numero de hosts unicos: "+hosts);
		
		System.out.println("\n2-) O total de erros 404: "+total404);
		
		System.out.println("\n3-) Os 5 URLs que mais causaram erro 404: ");
		urls404Code.forEach(t -> System.out.println(t._1+" - "+t._2));
		
		System.out.println("\n4-) Quantidade de erros 404 por dia:");
		urls404ToDay.forEach(t -> System.out.println(String.format("Dia: %s | Total: %d", t._1, t._2)));
		
		System.out.println("\n5-) Total de Bytes retornados: "+totalBytes);
		
	}
}
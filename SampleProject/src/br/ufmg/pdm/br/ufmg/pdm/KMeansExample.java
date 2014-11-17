package br.ufmg.pdm;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class KMeansExample {

	private static final String APP_NAME = "PDM - Trabalho Gustavo e Jacqueline";
	private static final String PATH_DATA = "/user/root/*.csv";
	private static final String PATH_MAP_BY_JOB_ID = "/user/root/mapJobId.txt";

	private static final Integer NUMERO_DE_COLUNAS = 18;
	
	private static final Integer TEMPO_INICIAL = 0;
	private static final Integer TEMPO_FINAL = 1;
	private static final Integer JOB_ID = 2;
	private static final Integer CPU_USAGE = 5;
	private static final Integer MEMORY_USAGE = 6;
	private static final Integer ASSIGNED_MEMORY = 7;
	private static final Integer UNMAPPED_CACHE = 8;
	private static final Integer CACHE = 9;
	private static final Integer MAXIMUM_MEMORY = 10;
	private static final Integer MED_I_O = 11;
	private static final Integer MED_DISK_SPACE = 12;
	private static final Integer MAX_CPU = 13;
	private static final Integer MAX_I_O = 14;
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName(APP_NAME);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	
	    generateFileMapJobIdByResources(sc);
	}

	private static void generateFileMapJobIdByResources(JavaSparkContext sc) {
		JavaRDD<String> data = sc.textFile(PATH_DATA);
	    JavaRDD<String> arrayByLine = functionArrayByLine(data);
	    JavaPairRDD<String, Vector> mapJobId = functionMapByJobId(arrayByLine);
	    JavaPairRDD<String, Vector> jobIdResources = functionReduceByJobId(mapJobId);
	
	    jobIdResources.saveAsTextFile(PATH_MAP_BY_JOB_ID);
	}

	private static JavaPairRDD<String, Vector> functionReduceByJobId(JavaPairRDD<String, Vector> mapJobId) {
		
		return mapJobId.reduceByKey(new Function2<Vector, Vector, Vector>() {
			
			private static final long serialVersionUID = -367498673880237884L;

			public Vector call(Vector a, Vector b) {
            	double array_a[] = a.toArray();
            	double array_b[] = b.toArray();
            	
            	double[] values = new double[array_a.length];
				int indexClasse = array_a.length - 1;
				for (int i = 0; i < indexClasse; i++){
					double somatorio = array_a[i] + array_b[i];
					values[i] = somatorio;
		
				}
            	double classe = array_a[indexClasse] + array_b[indexClasse];
            	values[indexClasse] = classe > 2 ? 2 : classe;
            	
                return Vectors.dense(values);
            }
	    }, 1);
	}

	private static JavaPairRDD<String, Vector> functionMapByJobId(JavaRDD<String> arrayByLine) {
		
		return arrayByLine.mapToPair(new PairFunction<String, String, Vector>() {

			private static final long serialVersionUID = 7160324314448446476L;

			public Tuple2<String, Vector> call(String s) {
	        	String[] sarray = s.split(",");
	        	double[] values = getArrayResources(sarray);
	        	Vector vector = Vectors.dense(values);
	            return new Tuple2<String, Vector>(sarray[0], vector);
	        }

			private double[] getArrayResources(String[] sarray) {
				
				double[] values = new double[sarray.length];
				for (int i = 0; i < sarray.length; i++){
					values[i]=sarray[i].equals("") ? 0 :  Double.parseDouble(sarray[i]);
				}
	        	values[0] = Double.parseDouble(sarray[1]);
	        	values[1]= Double.parseDouble(sarray[2]);
	        	values[values.length - 1]= 1;
				return values;
			}
	    });
	}

	private static JavaRDD<String> functionArrayByLine(JavaRDD<String> data) {
		return data.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 7689214802888112748L;

			public Iterable<String> call(String s) {
		  	    String[] sarray = s.split(",");
				String[] values = new String[sarray.length/NUMERO_DE_COLUNAS];
				int contIndexArrayByLine = 0;
				for (int index = 0; index < sarray.length; index+=NUMERO_DE_COLUNAS) {
					String textLine = getTextLine(sarray, index);
					values[contIndexArrayByLine] = textLine;
					contIndexArrayByLine++;
				}
	  	      	return Arrays.asList(values);
	        }

			private String getTextLine(String[] sarray, int index) {
				String textLine = sarray[index + JOB_ID] + "," +
						getTempoExecucao(sarray, index) +
						sarray[index + CPU_USAGE] + "," +
						sarray[index + MAX_CPU] + "," +
						sarray[index + MEMORY_USAGE] + "," +
						sarray[index + MAXIMUM_MEMORY] + "," +
						sarray[index + ASSIGNED_MEMORY] + "," +
						sarray[index + CACHE] + "," +
						sarray[index + UNMAPPED_CACHE] + "," +
						sarray[index + MED_DISK_SPACE] + "," +
						sarray[index + MED_I_O] + "," +
						sarray[index + MAX_I_O];
									
				return textLine;
			}
			
			private String getTempoExecucao(String[] sarray, int index){
				Double inicio =  Double.parseDouble(sarray[index + TEMPO_INICIAL]);
				Double fim = Double.parseDouble(sarray[index + TEMPO_FINAL]);
				
				Double tempoTotal = fim - inicio;
				
				return tempoTotal.toString();
			}
	    });
	}
}

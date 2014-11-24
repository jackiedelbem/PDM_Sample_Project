package br.ufmg.pdm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;
import scala.collection.Iterator;

import com.google.common.collect.Lists;

public class KMeansExample {

	
	private static final String PATH_REGISTROS_BY_CLASSE = "/user/root/resultados/numeroRegistroPorClasse.txt";
	private static final String APP_NAME = "PDM - Trabalho Gustavo e Jacqueline";
	private static final String PATH_DATA = "/user/root/*.csv";
	private static final String PATH_MAP_POR_CLASSE = "/user/root/resultados/mapPorClasse.txt";
	private static final String PATH_MAP_BY_JOB_ID = "/user/root/resultados/recursosById.txt";
	private static final String PATH_GRUPO_CLUSTER = "/user/root/resultados/grupoClusters.txt";
	private static final String PATH_CENTROIDES_PARCIAL = "/user/root/resultados/centroides_Parcial.txt";
	private static final String PATH_CENTROIDES = "/user/root/resultados/centroides.txt";
//	private static final String PATH_ESTATISTICA = "/user/root/resultado/estatistica.txt";

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
	
	public static void main(String[] args) 
	{
		SparkConf conf = new SparkConf().setAppName(APP_NAME);
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> data = sc.textFile(PATH_DATA);
	    generateFiles(data, sc);
	    
	}
	
	private static void generateFiles(JavaRDD<String> data, JavaSparkContext sc) 
	{
		JavaPairRDD<String, Vector> jobs = generateResoucesByJobId(data);
	    calculaKmeans(jobs, sc);
	}

	private static JavaPairRDD<String, Vector> generateResoucesByJobId(	JavaRDD<String> data)
	{
		JavaRDD<String> arrayByLine = functionArrayByLine(data);
	    JavaPairRDD<String, Vector> mapJobId = functionMapByJobId(arrayByLine);
	    JavaPairRDD<String, Vector> recursosByJobID = functionReduceByJobId(mapJobId);
	    JavaPairRDD<String, Vector> reduceJobByClasse = functionReduceJobByClasse(recursosByJobID);
	    JavaPairRDD<String, Integer> reduceByClasse = functionReduceByClasse(reduceJobByClasse);

	    recursosByJobID.saveAsTextFile(PATH_MAP_BY_JOB_ID);
	    reduceJobByClasse.saveAsTextFile(PATH_MAP_POR_CLASSE);
	    reduceByClasse.saveAsTextFile(PATH_REGISTROS_BY_CLASSE);
		
	    return reduceJobByClasse;
	}
	



	private static void calculaKmeans(JavaPairRDD<String, Vector> jobs, JavaSparkContext sc)
	{
		final List<Vector> centroids = inicializaCentroides(jobs);
		JavaPairRDD<Integer, Vector> closest = calculaPontoMaisProximo(jobs, centroids);
		JavaPairRDD<Integer, Iterable<Vector>> pointsGroup = closest.groupByKey();
		pointsGroup.saveAsTextFile(PATH_GRUPO_CLUSTER);
		JavaPairRDD<Integer, List<Vector>> newCentroids = calculaCentroidByCluster(pointsGroup);
		newCentroids.saveAsTextFile(PATH_CENTROIDES_PARCIAL);
		
		List<Vector> centroides_final = retornaCentroidesFinal(newCentroids,sc);
	}
	

	private static List<Vector> retornaCentroidesFinal(	JavaPairRDD<Integer, List<Vector>> newCentroids, JavaSparkContext sc) {
		List<Vector> centroides = new ArrayList<Vector>();
		for(Tuple2<Integer, List<Vector>> tupla : newCentroids.collect()){
			centroides.addAll(tupla._2);
		}
		
		return centroides;
	}

	private static JavaPairRDD<Integer, List<Vector>> calculaCentroidByCluster(JavaPairRDD<Integer, Iterable<Vector>> pointsGroup) 
	{
		JavaPairRDD<Integer, List<Vector>> newCentroids = pointsGroup.mapValues( new Function<Iterable<Vector>, List<Vector>>() 
				{
					private static final long serialVersionUID = 1L;
	
					public List<Vector> call(Iterable<Vector> ps) throws Exception {
						return average(ps);
					}
				});
		return newCentroids;
	}

	private static JavaPairRDD<Integer, Vector> calculaPontoMaisProximo(JavaPairRDD<String, Vector> jobs,final List<Vector> centroids) 
	{
		JavaPairRDD<Integer, Vector> closest = jobs.mapToPair(new PairFunction<Tuple2<String, Vector>, Integer, Vector>() 
				{
					private static final long serialVersionUID = -6789983016973806171L;
					public Tuple2<Integer, Vector> call(Tuple2<String, Vector> in) throws Exception {
				         return new Tuple2<Integer, Vector>(closestPoint(in._2(), centroids), in._2());
				       }
			     }
		   );
		return closest;
	}

	private static List<Vector> inicializaCentroides(JavaPairRDD<String, Vector> jobs) 
	{
		int K = 18;
		List<Tuple2<String, Vector>> centroidTuples = jobs.takeSample(false, K, 42);
		final List<Vector> centroids = Lists.newArrayList();
		for (Tuple2<String, Vector> t: centroidTuples) {
			centroids.add(t._2());
		}
		return centroids;
	}
	
	static List<Vector> average(Iterable<Vector> iterableVector) 
	{
		List<Vector> listaVector = Lists.newArrayList(iterableVector);
		int tamanhoArray = listaVector.get(0).toArray().length;
		
		double[] mediaClasse1 = new double[tamanhoArray];
		double[] mediaClasse2 = new double[tamanhoArray];
		double[] mediaClasse3 = new double[tamanhoArray];
		
		int registros_Classe1 = 0;
		int registros_Classe2 = 0;
		int registros_Classe3 = 0;
		
		for(Vector vector : listaVector){
			double[] arrayVector = vector.toArray();
			
			if(arrayVector[arrayVector.length - 1] == 1d){
				mediaClasse1 = somaArrays(arrayVector, mediaClasse1);
				registros_Classe1++;
			}
			else if (arrayVector[arrayVector.length - 1] == 2d){
				mediaClasse2 = somaArrays(arrayVector, mediaClasse2);
				registros_Classe2++;
			}
			else{
				mediaClasse3 = somaArrays(arrayVector, mediaClasse3);
				registros_Classe3++;
			}
		}
		
		List<Vector> listaCentroides = new ArrayList<Vector>();
		
		if(registros_Classe1 > 0)
			listaCentroides.add(calculaCentroide(mediaClasse1, registros_Classe1));
		
		if(registros_Classe2 > 0)
			listaCentroides.add(calculaCentroide(mediaClasse2, registros_Classe2));
		
		if(registros_Classe3 > 0)
			listaCentroides.add(calculaCentroide(mediaClasse3, registros_Classe3));
		
		return listaCentroides;
		
	}
	
	private static Vector calculaCentroide(double[] media,	int numero_registros) {
		for(int index = 0; index < media.length; index++)
			media[index] = media[index]/numero_registros;
		return Vectors.dense(media);
	}

	private static double[] somaArrays(double[] valores,double[] media) 
	{
		for(int index =0; index < media.length; index++)
			media[index] = media[index] + valores[index];
		return media;
	}

	static int closestPoint(Vector p, List<Vector> centers) 
	{
	    int bestIndex = 0;
	    double closest = Double.POSITIVE_INFINITY;
	    for (int i = 0; i < centers.size(); i++) {
	    	double tempDist = calculaDistanciaEuclidiana(p.toArray(), centers.get(i).toArray());
	    	if (tempDist < closest) {
	    		closest = tempDist;
	    		bestIndex = i;
	    	}
	    }
	    return bestIndex;
	}
	
	static double calculaDistanciaEuclidiana(double[] vector1, double[] vector2) 
	{
	     double sum = 0.0;
	     for (int index=0; index<vector1.length; index++) {
	    	 double diferenca = vector1[index] - vector2[index];
	          sum = sum + Math.pow(diferenca,2);
	     }
	     
	     return Math.pow(sum,0.5);
	}
	
	private static JavaPairRDD<String, Integer> functionReduceByClasse(JavaPairRDD<String, Vector> recursosJobByClasse) 
	{
		
		JavaPairRDD<String, Integer> classes = recursosJobByClasse.mapToPair(new PairFunction<Tuple2<String,Vector>, String, Integer>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(Tuple2<String, Vector> arg0) throws Exception {
				double[] arrayValues = arg0._2.toArray();
				int indexClasse = arrayValues.length - 1;
				String chave = "" + arrayValues[indexClasse];
			
				return new Tuple2<String, Integer>(chave, 1);
			}
			
		});
		
		return classes.reduceByKey(new Function2<Integer, Integer, Integer>() {
					
			private static final long serialVersionUID = -367498673880237884L;

			public Integer call(Integer a, Integer b) {
            	
                return a + b;
            }
	    }, 1);
	}
	

	private static JavaPairRDD<String, Vector> functionReduceJobByClasse(JavaPairRDD<String, Vector> recursosByJobID) 
	{
		
		JavaPairRDD<String, Vector> jobs = recursosByJobID.mapToPair(new PairFunction<Tuple2<String,Vector>, String, Vector>() {

			private static final long serialVersionUID = 1L;

			public Tuple2<String, Vector> call(Tuple2<String, Vector> arg0) throws Exception {
				String chave = arg0._1().split(",")[0];
				
				double[] arrayValues = arg0._2.toArray();
				
				int countIndex = 0;
				
				double[] arrayNew = new double[arrayValues.length - 3];
				for(int index = 3; index < arrayValues.length; index++){
					arrayNew[countIndex] = arrayValues[index];
					countIndex ++;
				}
				
			
				return new Tuple2<String, Vector>(chave, Vectors.dense(arrayNew));
			}
			
		});
		
//		return jobs;
		
		return jobs.reduceByKey(new Function2<Vector, Vector, Vector>() {
			
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
            	values[indexClasse] = 3;
				
                return Vectors.dense(values);
            }
	    }, 1);
	}
	

	private static JavaPairRDD<String, Vector> functionReduceByJobId(JavaPairRDD<String, Vector> mapJobId) {
		
		return mapJobId.reduceByKey(new Function2<Vector, Vector, Vector>() {
			
			private static final long serialVersionUID = -367498673880237884L;

			public Vector call(Vector a, Vector b) {
            	double array_a[] = a.toArray();
            	double array_b[] = b.toArray();
            	
            	double[] values = new double[array_a.length];
				int indexClasse = array_a.length - 1;
				for (int i = 3; i < indexClasse; i++){
					double somatorio = array_a[i] + array_b[i];
					values[i] = somatorio;
		
				}
				double classe = array_a[indexClasse] + array_b[indexClasse];
				//job_id e tempos
				values[0] = array_a[0];
				values[1] = array_a[1];
				values[2] = array_a[2];
				
				//classe
            	values[indexClasse] = classe >= 2 ? 2 : 1;
				
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
	            String chave = getChave(sarray);
				return new Tuple2<String, Vector>(chave, vector);
	        }
			
			private String getChave(String[] sarray){
				//chave composto pelo JobId, tempo inicial, tempo final
				return sarray[0] + "," + sarray[1] + "," + sarray[2];
			}

			private double[] getArrayResources(String[] sarray) {
				
				double[] values = new double[sarray.length];
				int index = 0;
				for (int i = 0; i < sarray.length; i++){
					values[index]=sarray[i].equals("") ? 0 :  Double.parseDouble(sarray[i]);
					index++;
				}
				return values;
			}
	    });
	}

	private static JavaRDD<String> functionArrayByLine(JavaRDD<String> data) {
		return data.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 7689214802888112748L;

			public Iterable<String> call(String s) {
		  	    String[] sarray = s.split("\n");
				String[] values = new String[sarray.length];
				int contIndexArrayByLine = 0;
				for (String line : sarray) {
					String[] arrayLine = line.split(",");
					values[contIndexArrayByLine] = getTextLine(arrayLine);;
					contIndexArrayByLine++;
				}
	  	      	return Arrays.asList(values);
	        }

			private String getTextLine(String[] sarray) {
				String textLine = sarray[JOB_ID] + "," +
						getValueArray(sarray, TEMPO_INICIAL) + "," +
						getValueArray(sarray, TEMPO_FINAL) + "," +
						getTempoExecucao(sarray) + "," +
						getValueArray(sarray,CPU_USAGE) + "," +
						getValueArray(sarray,MAX_CPU)	+ "," +
						getValueArray(sarray,MEMORY_USAGE) + "," +
						getValueArray(sarray,MAXIMUM_MEMORY) + "," +
						getValueArray(sarray,ASSIGNED_MEMORY) + "," +
						getValueArray(sarray,CACHE) + "," +
						getValueArray(sarray,UNMAPPED_CACHE) + "," +
						getValueArray(sarray,MED_DISK_SPACE) + "," +
						getValueArray(sarray,MED_I_O) + "," +
						getValueArray(sarray,MAX_I_O) + "," + 
						"1";
				return textLine;
			}
			
			private String getValueArray(String[] sarray, Integer index){
				try{
					return sarray[index].isEmpty() ? "0" : sarray[index];
				}catch(Exception ex){
					return "0";
				}
			}
			
			private Long getTempoExecucao(String[] sarray)
			{
				Long inicio =  Long.parseLong(sarray[TEMPO_INICIAL]);
				Long fim = Long.parseLong(sarray[TEMPO_FINAL]);
				Long variacaoTempo = fim - inicio;
				return variacaoTempo;
				//return TimeUnit.MILLISECONDS.toHours(variacaoTempo) ;
			}
	    });
	}
}
package br.ufmg.pdm;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

import com.google.common.collect.Lists;

public class KMeansExample {

	private static final String APP_NAME = "PDM - Trabalho Gustavo e Jacqueline";
	private static final String PATH_DATA = "/user/root/*.csv";
	private static final String PATH_MAP_BY_JOB_ID = "/user/root/recursosById.txt";
	private static final String PATH_GRUPO_CLUSTER = "/user/root/grupoClusters.txt";
	private static final String PATH_CENTROIDES = "/user/root/centroides.txt";

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
	    generateFiles(data);
	}
	
	private static void generateFiles(JavaRDD<String> data) 
	{
		JavaPairRDD<String, Vector> recursosByJobID = generateResoucesByJobId(data, PATH_MAP_BY_JOB_ID);
	    calculaKmeans(recursosByJobID);
	}

	private static JavaPairRDD<String, Vector> generateResoucesByJobId(	JavaRDD<String> data, String path)
	{
		JavaRDD<String> arrayByLine = functionArrayByLine(data);
	    JavaPairRDD<String, Vector> mapJobId = functionMapByJobId(arrayByLine);
	    JavaPairRDD<String, Vector> recursosByJobID = functionReduceByJobId(mapJobId);
	    recursosByJobID.saveAsTextFile(path);
		return recursosByJobID;
	}
	
	private static void calculaKmeans(JavaPairRDD<String, Vector> recursosByJobID)
	{
		final List<Vector> centroids = inicializaCentroides(recursosByJobID);
		JavaPairRDD<Integer, Vector> closest = calculaPontoMaisProximo(recursosByJobID, centroids);
		JavaPairRDD<Integer, Iterable<Vector>> pointsGroup = closest.groupByKey();
		pointsGroup.saveAsTextFile(PATH_GRUPO_CLUSTER);
		JavaPairRDD<Integer, Vector> newCentroids = calculaCentroidByCluster(pointsGroup);
		newCentroids.saveAsTextFile(PATH_CENTROIDES);
	}

	private static JavaPairRDD<Integer, Vector> calculaCentroidByCluster(JavaPairRDD<Integer, Iterable<Vector>> pointsGroup) 
	{
		JavaPairRDD<Integer, Vector> newCentroids = pointsGroup.mapValues( new Function<Iterable<Vector>, Vector>() 
				{
					private static final long serialVersionUID = 1L;
	
					public Vector call(Iterable<Vector> ps) throws Exception {
						return average(ps);
					}
				});
		return newCentroids;
	}

	private static JavaPairRDD<Integer, Vector> calculaPontoMaisProximo(JavaPairRDD<String, Vector> recursosByJobID,final List<Vector> centroids) 
	{
		JavaPairRDD<Integer, Vector> closest = recursosByJobID.mapToPair(new PairFunction<Tuple2<String, Vector>, Integer, Vector>() 
				{
					private static final long serialVersionUID = -6789983016973806171L;
					public Tuple2<Integer, Vector> call(Tuple2<String, Vector> in) throws Exception {
				         return new Tuple2<Integer, Vector>(closestPoint(in._2(), centroids), in._2());
				       }
			     }
		   );
		return closest;
	}

	private static List<Vector> inicializaCentroides(JavaPairRDD<String, Vector> recursosByJobID) 
	{
		int K = 18;
		List<Tuple2<String, Vector>> centroidTuples = recursosByJobID.takeSample(false, K, 42);
		final List<Vector> centroids = Lists.newArrayList();
		for (Tuple2<String, Vector> t: centroidTuples) {
			centroids.add(t._2());
		}
		return centroids;
	}
	
	static Vector average(Iterable<Vector> iterableVector) 
	{
		List<Vector> listaVector = Lists.newArrayList(iterableVector);
		int tamanhoArray = listaVector.get(0).toArray().length;
		double[] media = new double[tamanhoArray];
		for(Vector vector : listaVector){
			double[] arrayVector = vector.toArray();
			for(int index =0; index < media.length; index++)
				media[index] = media[index] + arrayVector[index];
		}
		for(int index =0; index < media.length; index++)
			media[index] = media[index]/listaVector.size();
	
		return Vectors.dense(media);
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
				
				double[] values = new double[sarray.length-1];
				int index = 0;
				for (int i = 1; i < sarray.length; i++){
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
					String textLine = getTextLine(arrayLine);
					values[contIndexArrayByLine] = textLine;
					contIndexArrayByLine++;
				}
	  	      	return Arrays.asList(values);
	        }

			private String getTextLine(String[] sarray) {
				String textLine = sarray[JOB_ID] + "," +
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
				
				return TimeUnit.MILLISECONDS.toHours(variacaoTempo) ;
			}
	    });
	}
}

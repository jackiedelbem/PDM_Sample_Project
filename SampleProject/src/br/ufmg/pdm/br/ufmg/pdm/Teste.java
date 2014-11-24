package br.ufmg.pdm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class Teste {
	
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
		String s = "(11,[[6.0E8,9.728125000000001E-5,6.02725E-4,2.68575E-4,2.713125E-4,0.00193675,3.353375E-4,1.2146500000000002E-4,2.38425E-7,1.6688375E-6,1.1611874999999999E-4,2.0]])";
		String[] arrayLine = s.split("[");
		String[] arrayLine2 = s.split("],[");
//		String textLine = getTextLine(arrayLine);
	    System.out.println(arrayLine2.toString());
		

	}
	
	public static String getTextLine(String[] sarray) {
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
	
	private static String getValueArray(String[] sarray, Integer index){
		try{
			return sarray[index].isEmpty() ? "0" : sarray[index];
		}catch(Exception ex){
			return "0";
		}
	}
	
	private static Long getTempoExecucao(String[] sarray){
		Long inicio =  Long.parseLong(sarray[TEMPO_INICIAL]);
		Long fim = Long.parseLong(sarray[TEMPO_FINAL]);
		
		Long variacaoTempo = fim - inicio;
		
		Long tempoEmSegundos = TimeUnit.MILLISECONDS.toSeconds(variacaoTempo) ;
		//TimeUnit.MILLISECONDS.toMinutes(variacaoTempo);
		//TimeUnit.MILLISECONDS.toHours(variacaoTempo)
		
		return tempoEmSegundos;
	}

}

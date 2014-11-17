package br.ufmg.pdm;

import java.util.concurrent.TimeUnit;

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
		String s = "600000000,900000000,3418309,0,4155527081,0.001562,0.06787,0.07568,0.001156,0.001503,0.06787,2.861e-06,0.0001869,0.03967,0.0003567,2.445,0.007243,0,1,";
		String[] arrayLine = s.split(",");
		String textLine = getTextLine(arrayLine);
	    System.out.println(textLine);

	}
	
	public static String getTextLine(String[] sarray) {
		String textLine = sarray[JOB_ID] + "," +
				getTempoExecucao(sarray) + "," +
				sarray[CPU_USAGE] + "," +
				sarray[MAX_CPU] + "," +
				sarray[MEMORY_USAGE] + "," +
				sarray[MAXIMUM_MEMORY] + "," +
				sarray[ASSIGNED_MEMORY] + "," +
				sarray[CACHE] + "," +
				sarray[UNMAPPED_CACHE] + "," +
				sarray[MED_DISK_SPACE] + "," +
				sarray[MED_I_O] + "," +
				sarray[MAX_I_O];
							
		return textLine;
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

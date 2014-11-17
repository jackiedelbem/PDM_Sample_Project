package br.ufmg.pdm;

import java.util.Arrays;

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
		String s = "600000000,900000000,3418309,0,4155527081,0.001562,0.06787,0.07568,0.001156,0.001503,0.06787,2.861e-06,0.0001869,0.03967,0.0003567,2.445,0.007243,0,1,"
				+ "600000000,900000000,3418309,1,329150663,0.001568,0.06787,0.07556,0.0003195,0.0007,0.06787,5.722e-06,0.0001879,,0.0009289,2.1,0.005791,0,1,"
				+ "600000000,900000000,3418314,0,3938719206,0.0003071,0.08044,0.09521,0.0002823,0.0006704,0.08044,4.768e-06,0.0001841,0.02377,0.0007858,,,0,1,"
				+ "600000000,900000000,3418314,1,351618647,0.0003004,0.08044,0.09521,0.0005369,0.0008698,0.08044,9.537e-06,0.0001831,0.007919,0.002285,5.198,0.02038,0,1,"
				+ "600000000,900000000,3418319,1,257348783,0.0005188,0.07678,0.0874,0.0003376,0.000721,0.07678,0.0002041,0.01508,0.0009289,3.055,0.01007,0,1,";
		String[] sarray = s.split(",");
		String[] values = new String[sarray.length/18];
		int contIndexArrayByLine = 0;
		for (int index = 0; index < sarray.length; index+=19) {
			String textLine = getTextLine(sarray, index);
			values[contIndexArrayByLine] = textLine;
			contIndexArrayByLine++;
		}
	    System.out.println(Arrays.asList(values));

	}
	
	public static String getTextLine(String[] sarray, int index) {
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
	
	private static String getTempoExecucao(String[] sarray, int index){
		Double inicio =  Double.parseDouble(sarray[index + TEMPO_INICIAL]);
		Double fim = Double.parseDouble(sarray[index + TEMPO_FINAL]);
		
		Double tempoTotal = fim - inicio;
		
		return tempoTotal.toString();
	}

}

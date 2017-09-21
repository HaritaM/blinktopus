package dbse.fopj.blinktopus.test;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import dbse.fopj.blinktopus.api.managers.LogManager;
import dbse.fopj.blinktopus.api.sv.AqpSV;
import dbse.fopj.blinktopus.api.sv.ColSV;
import dbse.fopj.blinktopus.api.sv.RowSV;

public class Evaluation {
	
	private static void loadData(String O, String LI) {
		LogManager.getLogManager().loadData("dataset/"+O,
				"dataset/"+LI);
	}
	
	private static double[] experiment1(ColSV col, RowSV row, String table, String attr, double lower, double higher, int nQuery)
	{
		ArrayList<Long> logTimes = new ArrayList<>();
		ArrayList<Long> rowTimes = new ArrayList<>();
		ArrayList<Long> colTimes = new ArrayList<>();
		
		for (int i=0;i<nQuery;++i)
		{
			logTimes.add(LogManager.getLogManager().getTime(table, attr, lower, higher, "Exp1"));
		}
		System.out.println("\t\t\tLog ready");
		
		for (int i=0;i<nQuery;++i)
		{
			rowTimes.add(row.query(table, attr, lower, higher).getTimeSV());
		}
		System.out.println("\t\t\tRow ready");
		
		for (int i=0;i<nQuery;++i)
		{
			colTimes.add(col.query(table, attr, lower, higher).getTimeSV());
		}
		System.out.println("\t\t\tCol ready");
		
		double averageLog = logTimes.stream().mapToLong(Long::longValue).sum() / nQuery;		
		double averageRow = rowTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageCol = colTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		
		System.out.println("\n!!!! Result: LOG: "+averageLog+" ; ROW: "+averageRow+" ; COL: "+averageCol+" !!!\n");
		double[] res = {averageLog, averageRow, averageCol, 0};
		return res;
	}
	
	private static double[] experiment2(AqpSV aqp, ColSV col, RowSV row, String table, String attr, double lower, double higher, int nQuery)
	{
		ArrayList<Long> logTimes = new ArrayList<>();
		ArrayList<Long> rowTimes = new ArrayList<>();
		ArrayList<Long> colTimes = new ArrayList<>();
		ArrayList<Long> aqpTimes = new ArrayList<>();
		long start = 0;
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			LogManager.getLogManager().getCount(table, attr, lower, higher, false, "Exp2");
			logTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tLog ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			row.getCount(table, attr, lower, higher, false, "Exp2");
			rowTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tRow ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			col.getCount(table, attr, lower, higher, false, "Exp2");
			colTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tCol ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			aqp.query(table, attr, lower, higher, false);
			aqpTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tHistogram ready");
		
		double averageLog = logTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageRow = rowTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageCol = colTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageAQP = aqpTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		
		System.out.println("\n!!!! Result: LOG: "+averageLog+" ; ROW: "+averageRow+" ; COL: "+averageCol+" ; AQP: "+averageAQP+" !!!\n");
		double[] res = {averageLog, averageRow, averageCol, averageAQP};
		return res;
	}
	
	private static double[] experiment3(AqpSV aqp, ColSV col, RowSV row, String table, String attr, double lower, double higher, int nQuery)
	{
		ArrayList<Long> logTimes = new ArrayList<>();
		ArrayList<Long> rowTimes = new ArrayList<>();
		ArrayList<Long> colTimes = new ArrayList<>();
		ArrayList<Long> aqpTimes = new ArrayList<>();
		long start = 0;
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			LogManager.getLogManager().getCount(table, attr, lower, higher, true, "Exp3");
			logTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tLog ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			row.getCount(table, attr, lower, higher, true, "Exp3");
			rowTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tRow ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			col.getCount(table, attr, lower, higher, true, "Exp3");
			colTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tCol ready");
		
		for (int i=0;i<nQuery;++i)
		{
			start = System.nanoTime();
			aqp.query(table, attr, lower, higher, true);
			aqpTimes.add(System.nanoTime()-start);
		}
		System.out.println("\t\t\tHLL ready");
		
		double averageLog = logTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageRow = rowTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageCol = colTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		double averageAQP = aqpTimes.stream().mapToLong(Long::longValue).sum() / nQuery;
		
		
		System.out.println("\n!!!! Result: LOG: "+averageLog+" ; ROW: "+averageRow+" ; COL: "+averageCol+" ; AQP: "+averageAQP+" !!!\n");
		double[] res = {averageLog, averageRow, averageCol, averageAQP};
		return res;
	}

	private static final String HEADER = "log, row, col, aqp";
	private static final String LINEBREAK = "\n";
	private static final String COMMA = ",";

	private static void exportData(ArrayList<double[]> res) {
		FileWriter fileWriter = null;

		String filepath = "evaluation.csv";

		try {
			fileWriter = new FileWriter(filepath);

			fileWriter.append(HEADER);
			fileWriter.append(LINEBREAK);

			for (int i = 0; i < res.size(); ++i) {
				fileWriter.append(String.valueOf(res.get(i)[0]));
				fileWriter.append(COMMA);
				fileWriter.append(String.valueOf(res.get(i)[1]));
				fileWriter.append(COMMA);
				fileWriter.append(String.valueOf(res.get(i)[2]));
				fileWriter.append(COMMA);
				fileWriter.append(String.valueOf(res.get(i)[3]));
				fileWriter.append(LINEBREAK);
			}

			System.out.println("CSV was created successfully");
		} catch (Exception e) {
			System.out.println("Error in CSV");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing");
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		//Evaluation Setup
//		String O_1 = "O_0.1.tbl";
//		String LI_1 = "LI_0.1.tbl";
		
//		String O_2 = "O_0.5.tbl";
//		String LI_2 = "LI_0.5.tbl";
		
//		String O_1 = "O_1.tbl";
//		String LI_1 = "LI_1.tbl";
//		
		String O_2 = "O_10.tbl";
		String LI_2 = "LI_10.tbl";
		
		String table = "orders";
		String attr = "totalprice";
//		
//		double lower1_10 = 800;
//		double higher1_10 = 36611;
//		
//		double lower1_90 = 800;
//		double higher1_90 = 257744;
//		
//		double lower2_10 = 800;
//		double higher2_10 = 37512;
//		
//		double lower2_90 = 800;
//		double higher2_90 = 264458;
		
//		double lower1_10 = 800;
//		double higher1_10 = 38769;
//		
//		double lower1_90 = 800;
//		double higher1_90 = 273702;
//		
		double lower2_10 = 800;
		double higher2_10 = 40000;
//		
		double lower2_90 = 800;
		double higher2_90 = 280000;
		
		int nQuery = 100;
		
//		System.out.println("Start SF1");
//		loadData(O_1,LI_1);
//		System.out.println("\tStart Experiment 1");
//		System.out.println("\t\tStart 10%");
//		ColSV col = new ColSV("Col3", table, attr, lower1_10, higher1_10);
//		RowSV row = new RowSV("Row4", table, attr, lower1_10, higher1_10);
//		double[] res1_1 = experiment1(col, row, table, attr, lower1_10, higher1_10, nQuery);
//		System.out.println("\t\tStart 90%");
//		col = new ColSV("Col3", table, attr, lower1_90, higher1_90);
//		row = new RowSV("Row4", table, attr, lower1_90, higher1_90);
//		double[] res1_2 = experiment1(col, row, table, attr, lower1_90, higher1_90, nQuery);
		
		System.out.println("Start SF2");
		loadData(O_2,LI_2);
		System.out.println("\tStart Experiment 1");
		System.out.println("\t\tCreate ColSV for 10%");
		ColSV col = new ColSV("Col1", table, attr, lower2_10, higher2_10);
		System.out.println("\t\tCreate RowSV for 10%");
		RowSV row = new RowSV("Row2", table, attr, lower2_10, higher2_10);
		System.out.println("\t\t\tStart 10%");
		double[] res1_3 = experiment1(col, row, table, attr, lower2_10, higher2_10, nQuery);
		System.out.println("\t\tCreate ColSV for 90%");
		col = new ColSV("Col1", table, attr, lower2_90, higher2_90);
		System.out.println("\t\tCreate RowSV for 90%");
		row = new RowSV("Row2", table, attr, lower2_90, higher2_90);
		System.out.println("\t\t\tStart 90%");
		double[] res1_4 = experiment1(col, row, table, attr, lower2_90, higher2_90, nQuery);
		System.out.println("\tStart Experiment 2");
		System.out.println("\t\tCreate histogram and sketch");
		AqpSV aqp = new AqpSV();
		System.out.println("\t\tCreate ColSV");
		ColSV countCol = new ColSV("Col1",table,attr,Double.MIN_VALUE,Double.MAX_VALUE);
		System.out.println("\t\tCreate RowSV");
		RowSV countRow = new RowSV("Row2",table,attr,Double.MIN_VALUE,Double.MAX_VALUE);
		System.out.println("\t\t\tStart 10%");
		double[] res2_1 = experiment2(aqp,countCol,countRow, table, attr, lower2_10, higher2_10, nQuery);
		System.out.println("\t\t\tStart 90%");
		double[] res2_2 = experiment2(aqp,countCol,countRow, table, attr, lower2_90, higher2_90, nQuery);
		System.out.println("\tStart Experiment 3");
		double[] res3 = experiment3(aqp,countCol,countRow, table, attr, Double.MIN_VALUE, Double.MAX_VALUE, nQuery);
		
		System.out.println("Save results");
		ArrayList<double[]> res =new ArrayList<>();
//		res.add(res1_1);
//		res.add(res1_2);
		res.add(res1_3);
		res.add(res1_4);
		res.add(res2_1);
		res.add(res2_2);
		res.add(res3);
		
		System.out.println("Start generating csv");
		exportData(res);
	}
}

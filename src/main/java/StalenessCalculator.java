import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class StalenessCalculator {

	public static void main(String[] args){
		String csvFile = "ourLog.csv";
		BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        Long RSU1= 284539537934404L;
        Long RSU2= 284549541893250L;
        Long RSU3= 284559551908158L;
        List<String[]> updatesRSU1 = new ArrayList<>();
        List<String[]> updatesRSU2 = new ArrayList<>();
        List<Long> stalenesses = new ArrayList<>();
        List<Long> currency1 = new ArrayList<>();
        List<Long> staleness14 = new ArrayList<>();
        List<Long> obsolescence = new ArrayList<>();
        Set<String> updatedKeys1 = new HashSet<>();
        Set<String> updatedKeys2 = new HashSet<>();
        List<Float> freshnessRate = new ArrayList<>();
        List<Float> freshnessIndex = new ArrayList<>();
        
        long recordCount = 1000;
        int totalReads0 = 0;
        int absoluteStale0 = 0;
        int absoluteFresh0 = 0;
        int totalReads1 = 0;
        int absoluteStale1 = 0;
        int absoluteFresh1 = 0;
        int rsu1updates = 0;
        int rsu2updates = 0;
        long lastTLog = 1L;
        
        long tmaxtx = 0L;
        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] operation = line.split(cvsSplitBy);
                if (operation[0].equals("UPDATE")){
                	if(Long.parseLong(operation[2])<RSU2){
                		updatedKeys1.add(operation[1]);
                		rsu1updates++;
                		updatesRSU1.add(operation);
                		tmaxtx = Long.parseLong(operation[2]);
                	}
                	else{
                		updatedKeys2.add(operation[1]);
                		rsu2updates++;
                		updatesRSU2.add(operation);
                	}
                	lastTLog = Long.parseLong(operation[2]);
                }
                else if (operation[0].equals("READ")){
                	
                	if(Long.parseLong(operation[2])<RSU2){
                		obsolescence.add((long)rsu1updates);
                		totalReads0++;
                		currency1.add(Long.parseLong(operation[2])-RSU1);
                		freshnessRate.add((float) (100*(recordCount-updatedKeys1.size())/(float)recordCount));
                		boolean wasStale = false;
                		for (int i=updatesRSU1.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU1.get(i)[1])){
                				wasStale=true;
                				stalenesses.add(Long.parseLong(updatesRSU1.get(i)[2])-RSU1);
                				absoluteStale0++;
                				break;
                			}
                		}
                		if (!wasStale){
                			absoluteFresh0++;
                			stalenesses.add(0L);
                		}
                	}
                	else{
                		totalReads1++;
                		obsolescence.add((long)rsu2updates);
                		freshnessRate.add((float) (100*(recordCount-updatedKeys2.size())/(float)recordCount));
                		boolean wasStale = false;
                		currency1.add(Long.parseLong(operation[2])-RSU2);
            			staleness14.add(Long.parseLong(operation[2])-tmaxtx);
            			freshnessIndex.add(((float)tmaxtx/(float)lastTLog));
                		for (int i=updatesRSU2.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU2.get(i)[1])){
                				wasStale=true;
                				stalenesses.add(Long.parseLong(updatesRSU2.get(i)[2])-RSU2);
                				absoluteStale1++;
                				break;
                			}
                		}
                		if (!wasStale){
                			stalenesses.add(0L);
                			absoluteFresh1++;
                		}
                	}
                }
            }
            long average = 0L;
            float flaverage = 0f;
            int count = 0;

            System.out.println("Metrics with numbers...");
            System.out.println("1");
            System.out.println("For this one a plot is better than an average...");
            long averageCurrency1= 0L;
            count = 0;
            for (Long sl:currency1){
            	averageCurrency1+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            averageCurrency1=averageCurrency1/currency1.size();
            System.out.println("Your average currency, from "+currency1.size()+" measurements was:"+averageCurrency1);
            
            System.out.println("2");
            System.out.println("Absolute Freshness and Staleness measurements...");
            System.out.println("For first window (between 2 row store updates): ");
            System.out.println("Total reads: "+totalReads0+", AbsoluteFresh: "+absoluteFresh0+", AbsoluteStale: "+absoluteStale0);
            System.out.println("For second window (between 2 row store updates): ");
            System.out.println("Total reads: "+totalReads1+", AbsoluteFresh: "+absoluteFresh1+", AbsoluteStale: "+absoluteStale1);
            
            System.out.println("3");
            count = 0;
            average = 0;
            System.out.println("For this one a plot is better than an average...");
            for (Long sl:staleness14){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/staleness14.size();
            System.out.println("Your average staleness14, from "+staleness14.size()+" measurements was:"+average);
            
            System.out.println("4");
            count = 0;
            average = 0;
            System.out.println("For this one a plot is better than an average...");
            for (Long sl:obsolescence){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/obsolescence.size();
            System.out.println("Your average obsolescence, from "+obsolescence.size()+" measurements was:"+average);
            
            System.out.println("5");
            count = 0;
            flaverage = 0;
            System.out.println("For this one a plot is better than an average...");
            for (Float sl:freshnessRate){
            	flaverage+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            flaverage=flaverage/freshnessRate.size();
            System.out.println("Your average freshnessRate, from "+freshnessRate.size()+" measurements was:"+flaverage);
            
            System.out.println("6");
            average = 0;
            count=0;
            for (Long sl:stalenesses){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/stalenesses.size();
            System.out.println("Your average timeliness, from "+stalenesses.size()+" measurements was:"+average);
            
            
            System.out.println("9");
            flaverage = 0;
            count=0;
            for (Float sl:freshnessIndex){
            	flaverage+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            flaverage=flaverage/freshnessIndex.size();
            System.out.println("Your average freshnessIndex, from "+freshnessIndex.size()+" measurements was:"+flaverage);
            


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
}

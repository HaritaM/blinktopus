import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] operation = line.split(cvsSplitBy);
                if (operation[0].equals("UPDATE")){
                	if(Long.parseLong(operation[2])<RSU2){
                		updatesRSU1.add(operation);
                	}
                	else{
                		updatesRSU2.add(operation);
                	}
                }
                else if (operation[0].equals("READ")){
                	if(Long.parseLong(operation[2])<RSU2){
                		boolean wasStale = false;
                		for (int i=updatesRSU1.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU1.get(i)[1])){
                				wasStale=true;
                				stalenesses.add(Long.parseLong(updatesRSU1.get(i)[2])-RSU1);
                				break;
                			}
                		}
                		if (!wasStale){
                			stalenesses.add(0L);
                		}
                	}
                	else{
                		boolean wasStale = false;
                		for (int i=updatesRSU2.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU2.get(i)[1])){
                				wasStale=true;
                				stalenesses.add(Long.parseLong(updatesRSU2.get(i)[2])-RSU2);
                				break;
                			}
                		}
                		if (!wasStale){
                			stalenesses.add(0L);
                		}
                	}
                }
            }
            long average = 0L;
            int count = 0;
            for (Long sl:stalenesses){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/stalenesses.size();
            System.out.println("Your average staleness, from "+stalenesses.size()+" measurements was:"+average);
            

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

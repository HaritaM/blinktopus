import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StalenessCalculator {

	public static void main(String[] args){
		String csvFile = "ourLog.csv";
		Long windowSize = 2000000000L;
		Long tick = 2000000000L;
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
        List<Long> currency29 = new ArrayList<>();
        List<Long> staleness14 = new ArrayList<>();
        List<Long> timeliness8 = new ArrayList<>();
        List<Long> timeliness29 = new ArrayList<>();
        List<Long> obsolescence = new ArrayList<>();
        Set<String> updatedKeys1 = new HashSet<>();
        Set<String> updatedKeys2 = new HashSet<>();
        List<Float> freshnessRate = new ArrayList<>();
        List<Float> freshnessIndex = new ArrayList<>();
        List<Long> enhancedAveraging = new ArrayList<>();
        List<Long> shiftingWindow = new ArrayList<>();
        
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
        Map<String, List<Long>> updateTimes = new HashMap<>();
        long tmaxtx = 0L;
        boolean firstTime = true;
        Long start = 284516000000000L;
        Map<String, Integer> currUpdates  = new HashMap<>(); 
        float alpha = 0.8f;
        Map<String, Float> sav  = new HashMap<>(); 
        Map<String, Float> pti  = new HashMap<>(); 
        Map<String, Map<Long, Float>> historyOfPti = new HashMap<>();
        boolean firstBool = false;
        boolean secondBool = false;
        try {

            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] operation = line.split(cvsSplitBy);
                if (operation[0].equals("UPDATE")){
                	if (firstTime){
                		while (start+tick<=Long.parseLong(operation[2])){
                			start+=tick;
                		}
                		firstTime= false;
                	}
                	if (Long.parseLong(operation[2])>start+tick){
                		for (String key: currUpdates.keySet()){
                			firstBool = sav.get(key)>0;
                			secondBool = currUpdates.get(key)>0;
                			if (!firstBool){
                				pti.put(key, 0f);
                			}
                			else {
                				if (secondBool){
                    				pti.put(key, 0f);                					
                				}
                				else{
                					pti.put(key, pti.get(key)+1);
                					if (historyOfPti.containsKey(key)){
                						Map<Long, Float> hpt = historyOfPti.get(key);
                						hpt.put(start, pti.get(key)*tick);
                    					historyOfPti.put(key, hpt);
                					}
                					else{
                						Map<Long, Float> hpt = new LinkedHashMap<>();
                						hpt.put(start, pti.get(key)*tick);
                    					historyOfPti.put(key, hpt);
                					}
                				}
                				
                			}
                			sav.put(key, (1-alpha)*sav.get(key)+(alpha+currUpdates.get(key)));
                		}
                		for (String key: currUpdates.keySet()){
                			currUpdates.put(key, 0);
                		}
                		start = start+tick;
                	}
                	if(Long.parseLong(operation[2])<RSU2){
                		updatedKeys1.add(operation[1]);
                		if (currUpdates.containsKey(operation[1])){
                			currUpdates.put(operation[1], currUpdates.get(operation[1])+1);
                		}
                		else{
                			sav.put(operation[1], 0f);
                			pti.put(operation[1], 0f);
                			currUpdates.put(operation[1], 1);
                		}
                		rsu1updates++;
                		updatesRSU1.add(operation);
                		tmaxtx = Long.parseLong(operation[2]);
                	}
                	else{
                		updatedKeys2.add(operation[1]);
                		rsu2updates++;
                		if (currUpdates.containsKey(operation[1])){
                			currUpdates.put(operation[1], currUpdates.get(operation[1])+1);
                		}
                		else{
                			sav.put(operation[1], 0f);
                			pti.put(operation[1], 0f);
                			currUpdates.put(operation[1], 1);
                		}
                		updatesRSU2.add(operation);
                	}
                	
            		List<Long> times = new ArrayList<>();
                	if (updateTimes.containsKey(operation[1])){
                		times = updateTimes.get(operation[1]);
                	}
            		times.add(Long.parseLong(operation[2]));
            		updateTimes.put(operation[1], times);
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
                    	if (updateTimes.containsKey(operation[1])){
                    		int itemCount =0;
                    		Long firstItem = 0L;
                    		Long lastItem = 0L;
                    		Long predictedUpdate = 0L;
                    		for (int i=0; i<updateTimes.get(operation[1]).size(); i++){
                    			if (updateTimes.get(operation[1]).get(i)<=RSU1){
                    				itemCount++;
                    				lastItem =updateTimes.get(operation[1]).get(i);
                    				if (firstItem==0L){
                    					firstItem = updateTimes.get(operation[1]).get(i);
                    				}
                    			}
                    			else{
                    				if (itemCount>0){
                    					predictedUpdate = lastItem + (lastItem-firstItem)/itemCount;
                    				}
                    				break;
                    			}
                    		}
                    		Long tCurrent = Long.parseLong(operation[2]);
        					Long windowStart = 284516000000000L;
                    		while (windowStart<=Long.parseLong(operation[2])){
                    			windowStart+=windowSize;
                    		}
                    		Long windowEnd = windowStart+windowSize;
                    		Long windowBefore = windowStart-windowSize;
                    		Long ncur = 0L;
                    		Long nprev = 0L;
                    		Long predictedUpdateShiftingWindow = 0L;
                    		for (int k=0; k< updateTimes.get(operation[1]).size(); k++){
                    			if (updateTimes.get(operation[1]).get(k)<=Long.parseLong(operation[2]) && updateTimes.get(operation[1]).get(k)>=windowStart){
                    				ncur++;
                    			}
                    			else if (updateTimes.get(operation[1]).get(k)<=windowStart && updateTimes.get(operation[1]).get(k)>=windowBefore){
                    				nprev++;
                    			}
                    		}

                    		if (ncur>=3){
                    			predictedUpdateShiftingWindow = lastItem + tCurrent-windowStart/ncur;
                    			shiftingWindow.add(tCurrent-predictedUpdateShiftingWindow);
                    		}
                    		else{
                    			if (nprev>0){predictedUpdateShiftingWindow = lastItem + windowSize/nprev; shiftingWindow.add(tCurrent-predictedUpdateShiftingWindow);}
                    		}
                    		if (predictedUpdate!=0L){
                    		if (tCurrent>predictedUpdate){
                    			enhancedAveraging.add(tCurrent-predictedUpdate);
                    		}
                    		else{
                    			enhancedAveraging.add(0L);
                    		}}
                    	}
                	}
                	else{
                		totalReads1++;
                		obsolescence.add((long)rsu2updates);
                		freshnessRate.add((float) (100*(recordCount-updatedKeys2.size())/(float)recordCount));
                		boolean wasStale = false;
                		currency1.add(Long.parseLong(operation[2])-RSU2);
                		
                		long currentCurrency = Long.parseLong(operation[2])-RSU2;
                		long newage = 0L;
                		long secondUpdate = 0L;
                		long volatility = 0L;
                		for (int i=updatesRSU1.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU1.get(i)[1])){
                				newage = RSU2-Long.parseLong(updatesRSU1.get(i)[2]);
                				timeliness8.add(Long.parseLong(operation[2])-Long.parseLong(updatesRSU1.get(i)[2]));
                				break;
                			}
                		}
                		for (int i=updatesRSU1.size()-1; i>0; i--){
                			if (operation[1].equals(updatesRSU1.get(i)[1])){
                				if (secondUpdate>0L){
                					volatility= secondUpdate - Long.parseLong(updatesRSU1.get(i)[2]);
                					break;
                				}
                				else{
                					secondUpdate = Long.parseLong(updatesRSU1.get(i)[2]);
                				}
                				
                			}
                		}
                		if (newage>0L){
                			currentCurrency+=newage;
                			currency29.add(currentCurrency);
                			if (volatility>0L){
                				if (1>currentCurrency/volatility){
                					timeliness29.add(1-currentCurrency/volatility);
                				}
                				else{
                					timeliness29.add(0L);
                				}
                			}
                		}
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
                		if (updateTimes.containsKey(operation[1])){
                    		int itemCount =0;
                    		Long firstItem = 0L;
                    		Long lastItem = 0L;
                    		Long predictedUpdate = 0L;
                    		for (int i=0; i<updateTimes.get(operation[1]).size(); i++){
                    			if (updateTimes.get(operation[1]).get(i)<=RSU2){
                    				itemCount++;
                    				lastItem =updateTimes.get(operation[1]).get(i);
                    				if (firstItem==0L){
                    					firstItem = updateTimes.get(operation[1]).get(i);
                    				}
                    			}
                    			else{
                    				if (itemCount>0){
                    					predictedUpdate = lastItem + (lastItem-firstItem)/itemCount;
                    				}
                    				break;
                    			}
                    		}
                    		Long tCurrent = Long.parseLong(operation[2]);
        					Long windowStart = 284516000000000L;
                    		while (windowStart<=Long.parseLong(operation[2])){
                    			windowStart+=windowSize;
                    		}
                    		Long windowEnd = windowStart+windowSize;
                    		Long windowBefore = windowStart-windowSize;
                    		Long ncur = 0L;
                    		Long nprev = 0L;
                    		Long predictedUpdateShiftingWindow = 0L;
                    		for (int k=0; k< updateTimes.get(operation[1]).size(); k++){
                    			if (updateTimes.get(operation[1]).get(k)<=Long.parseLong(operation[2]) && updateTimes.get(operation[1]).get(k)>=windowStart){
                    				ncur++;
                    			}
                    			else if (updateTimes.get(operation[1]).get(k)<=windowStart && updateTimes.get(operation[1]).get(k)>=windowBefore){
                    				nprev++;
                    			}
                    		}

                    		if (ncur>=3){
                    			predictedUpdateShiftingWindow = lastItem + tCurrent-windowStart/ncur;
                    			shiftingWindow.add(tCurrent-predictedUpdateShiftingWindow);
                    		}
                    		else{
                    			if (nprev>0){predictedUpdateShiftingWindow = lastItem + windowSize/nprev; shiftingWindow.add(tCurrent-predictedUpdateShiftingWindow);}
                    		}
                    		if (predictedUpdate!=0L){
                    		if (tCurrent>predictedUpdate){
                    			enhancedAveraging.add(tCurrent-predictedUpdate);
                    		}
                    		else{
                    			enhancedAveraging.add(0L);
                    		}}
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
            
            
            System.out.println("11");
            average = 0;
            count=0;
            for (Long sl:currency29){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/currency29.size();
            System.out.println("Your average currency29, from "+currency29.size()+" measurements was:"+average);
            
            System.out.println("8");
            average = 0;
            count=0;
            for (Long sl:timeliness8){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/timeliness8.size();
            System.out.println("Your timeliness8, from "+timeliness8.size()+" measurements was:"+average);
            
            System.out.println("29, 31");
            average = 0;
            count=0;
            for (Long sl:timeliness29){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/timeliness29.size();
            System.out.println("Your timeliness29 and 31, from "+timeliness29.size()+" measurements was:"+average);
            
            System.out.println("Enhanced Averaging");
            average = 0;
            count=0;
            for (Long sl:enhancedAveraging){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/enhancedAveraging.size();
            System.out.println("Your enhancedAveraging, from "+enhancedAveraging.size()+" measurements was:"+average);
            
            System.out.println("Shifting window");
            average = 0;
            count=0;
            for (Long sl:shiftingWindow){
            	average+=sl;
            	System.out.println(count+","+sl);
            	count++;
            }
            average=average/shiftingWindow.size();
            System.out.println("Your shifting window, from "+shiftingWindow.size()+" measurements was:"+average);

            System.out.println("History of non zero stalenesses per key");
            historyOfPti.entrySet().forEach(entry->{
            	System.out.println("Key: "+entry.getKey());
            	entry.getValue().entrySet().forEach(hist->{
            		System.out.println(hist.getKey()+":"+hist.getValue());
            	});
            });

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

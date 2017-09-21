package dbse.fopj.blinktopus.api.sv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.yahoo.sketches.hll.HllSketch;

import dbse.fopj.blinktopus.api.datamodel.LineItem;
import dbse.fopj.blinktopus.api.datamodel.Order;
import dbse.fopj.blinktopus.api.managers.LogManager;

/**
 * AQP-module. Uses two types of synopses: histograms and sketches. Histogram: equi-depth. Sketch: HyperLogLog.
 * @author Hashaam
 *
 */
public class AqpSV extends SV{
	private List<Order> ordersData = new ArrayList<>();
	private List<LineItem> lineitemData = new ArrayList<>();
	
	private Map<Double,Integer> histOrders = new HashMap<>();
	private Map<Double,Integer> histLineItems = new HashMap<>();
	
	private List<Double> totalPriceData = new ArrayList<>();
	private final int lgK=12;
	private HllSketch sketch;
	
	/**
	 * Default constructor. Creates histograms for Order and LineItem tables (totalPrice, extendedPrice).
	 * Create sketch for Order table (totalprice).
	 */
	public AqpSV()
	{
		super("AQP", "aqp", "everything", "price", Double.MIN_VALUE, Double.MAX_VALUE);
		this.ordersData = (List<Order>)(List<?>)LogManager.getLogManager().scan("orders", "totalprice", Double.MIN_VALUE, Double.MAX_VALUE, "Orders histogram").getResultTuples();
		this.lineitemData = (List<LineItem>)(List<?>)LogManager.getLogManager().scan("lineitems", "extendedprice", Double.MIN_VALUE, Double.MAX_VALUE, "LineItems histogram").getResultTuples();
		histOrders = calculateHistogramsOrder(ordersData);
		histLineItems = calculateHistogramsLI(lineitemData);
		
		this.totalPriceData=ordersData.stream()
	              .map(Order::getTotalPrice)
	              .collect(Collectors.toList());
		this.sketch = new HllSketch(this.lgK);
		this.totalPriceData.forEach(item -> sketch.update(item));
	}

	private Map<Double, Integer> calculateHistogramsOrder(List <Order> histogramList){
		// sort the list on the attribute we are making Histogram on
		Collections.sort(histogramList, Comparator.comparing(Order::getTotalPrice));
		//calculate total Size
		int size = histogramList.size();
		// Race Rule to compute number of bins for histogram
		int bins = (int)(2*java.lang.Math.ceil(Math.pow(size, 0.33)));
		//calculate number of items per bin
		int itemsPerBin = (int)java.lang.Math.ceil((float)size/bins);
		Map<Double, Integer> histoTotalPrice = new HashMap<Double, Integer>();
		IntStream.range(0, bins).forEach(listItem -> {
			//outer loop to put data in each bin
			int startindex = listItem*itemsPerBin;
			int endindex = startindex+itemsPerBin;
			if (endindex>size){
				endindex = size;
			}
			double binStartValue = histogramList.get(startindex).getTotalPrice();
			List<Order> histoData = new ArrayList<Order>();
			IntStream.range(startindex, endindex).forEach(s -> {
				//get x items per bin and add them to the bin
				if(s<size){
					histoData.add(histogramList.get(s));
				}
			});
			//place count with bin starting values as key
			histoTotalPrice.put(binStartValue, histoData.size());
			//place the rightmost element of the histogram as last element in list
			histoTotalPrice.put(histogramList.get(histogramList.size()-1).getTotalPrice(), 0);
		});
		return histoTotalPrice;
	}
	
	private Map<Double, Integer> calculateHistogramsLI(List <LineItem> histogramList){
		// sort the list on the attribute we are making Histogram on
		Collections.sort(histogramList, Comparator.comparing(LineItem::getExtendedPrice));
		//calculate total Size
		int size = histogramList.size();
		// Race Rule to compute number of bins for histogram
		int bins = (int)(2*java.lang.Math.ceil(Math.pow(size, 0.33)));
		//calculate number of items per bin
		int itemsPerBin = (int)java.lang.Math.ceil((float)size/bins);
		Map<Double, Integer> histoTotalPrice = new HashMap<Double, Integer>();
		IntStream.range(0, bins).forEach(listItem -> {
			//outer loop to put data in each bin
			int startindex = listItem*itemsPerBin;
			int endindex = startindex+itemsPerBin;
			if (endindex>size){
				endindex = size;
			}
			double binStartValue = histogramList.get(startindex).getExtendedPrice();
			List<LineItem> histoData = new ArrayList<LineItem>();
			IntStream.range(startindex, endindex).forEach(s -> {
				//get x items per bin and add them to the bin
				if(s<size){
					histoData.add(histogramList.get(s));
				}
			});
			//place count with bin starting values as key
			histoTotalPrice.put(binStartValue, histoData.size());
			//place the rightmost element of the histogram as last element in list
			histoTotalPrice.put(histogramList.get(histogramList.size()-1).getExtendedPrice(), 0);
		});
		return histoTotalPrice;
	}
	
	private double closest(double of, List<Double> in) {
		// calculate the closest smallest element to given value
		double min = Double.MAX_VALUE;
		double closest = of;
	    for (double v : in) {
	        final double diff = Math.abs(v - of);
	        if (diff < min) {
	            min = diff;
	            closest = v;
	        }
	    }
	    return closest;
	}
	
	/**
	 * Returns approximate number of (unique) values in a given range.
	 * @param table Table to be queried. (Order/LineItem)
	 * @param attr Attribute to be queried (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param distinct True, if number of unique values to be returned, false otherwise.
	 * @return The approximate number of (unique) values in a given range.
	 */
	public long query(String table, String attr, double lower, double higher, boolean distinct)
	{
		if (!distinct)
		{
			return queryHist(table,attr,lower,higher);
		}
		else
		{
			return queryHLL();
		}
	}
	
	private long queryHLL()
	{
		return (long)this.sketch.getEstimate();
	}
	
	private long queryHist(String table, String attr, double lower, double higher)
	{
		if (table.toLowerCase().equals("orders"))
			return queryHistograms(lower,higher,histOrders);
		else
			return queryHistograms(lower,higher,histLineItems);
	}
	
	/*queryHistograms() takes the ranges as input and gives count as result*/
	private long queryHistograms(double startrange, double endrange, Map<Double, Integer> histoTotalPrice){
		long startTime = System.nanoTime();
		double count =0;
		//get all the keys from the Map and sort them
		List<Double> l = new ArrayList<Double>(histoTotalPrice.keySet());
		Collections.sort(l);
		//after sorting list, last element will always be rightmost value in data
		double rightMostValue = l.get(l.size()-1);
    	l.remove(l.size()-1);
    	//e.g l = [0.1, 33.33, 333.333, 3333.3333]
    	// get all the bins whose start value is greater than startrange and smaller than endrange
        List<Double> resultSure = l.stream().filter(s -> s >= startrange && s <= endrange).collect(Collectors.toList());
        double endbin;
        if(resultSure.isEmpty()==false){
        	//if starting and ending range both lie in different bins
	        if(l.indexOf(resultSure.get(0)) > 0 && resultSure.get(0)!=startrange && resultSure.get(0)!=endrange){
	        	//if startrange is 10, resultSure will contains bins from 33.33 onwards
	        	//so to include 0.1 and do approximation for the bin with start value 0.1
	        	double startbin = l.get(l.indexOf(resultSure.get(0))-1);
	        	count += ((resultSure.get(0)- startrange)/(resultSure.get(0)-startbin))*histoTotalPrice.get(startbin);
	        }
	        int tempsize = resultSure.size()-1;
	        if(endrange < rightMostValue){
	        	//if the endrange lies with in our data
	        	if(endrange != resultSure.get(tempsize)){
	        		/* if the endrange is not equal to the start value of last bin in resultSure
	        		 because we need results between 10-33.33 so if the next bin starts with 33.33
	        		 we wont need to include that */
			        if(l.indexOf(resultSure.get(tempsize))!=l.size()-1){
			        	//if element belongs to bin A then also get the next bin B to 
			        	//do the continuous value approximation
			        	endbin = l.get(l.indexOf(resultSure.get(tempsize))+1);
			            count += (endbin - endrange)/(endbin-resultSure.get(tempsize))*histoTotalPrice.get(resultSure.get(tempsize));
			            resultSure.remove(tempsize);	
			        }else{
			        	//if element is already in last bin and use rightmost value to calculate 
			        	//approximate query processing
			        	count += (rightMostValue - endrange)/(rightMostValue-resultSure.get(tempsize))*histoTotalPrice.get(resultSure.get(tempsize));
			            resultSure.remove(tempsize);
			        }
		        }else{
		        	/* if the endrange is equal to the start value of last bin in resultSure
	        		 then remove that as we wont want the count of that bin */
		        	resultSure.remove(tempsize);
		        }
	        }
	        for(Double key: resultSure){
	        	//count for all the bins who dont need approximations
	        	count += histoTotalPrice.get(key);
	        }
        }else{
        	//if starting and ending range both are inside one bin
        	double startbin = closest(startrange, l);
        	if(endrange>rightMostValue){
        		//if the end range is greater than the highest value in data
        		count += ((rightMostValue- startrange)/(rightMostValue-startbin))*histoTotalPrice.get(startbin);
        	}else{
        		count += ((endrange- startrange)/(endrange-startbin))*histoTotalPrice.get(startbin);
        	}
        }
        this.setTime(System.nanoTime()-startTime);
		return (long)count;
	}

	
}

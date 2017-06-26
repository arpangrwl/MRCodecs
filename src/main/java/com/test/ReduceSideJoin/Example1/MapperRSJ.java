package com.test.ReduceSideJoin.Example1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
 
 
public class MapperRSJ extends Mapper<LongWritable, Text, CompositeKeyWritableRSJ, Text> {
 
	CompositeKeyWritableRSJ ckwKey = new CompositeKeyWritableRSJ();
	Text txtValue = new Text("");
	int intSrcIndex = 0;
	StringBuilder strMapValueBuilder = new StringBuilder("");
	List<Integer> lstRequiredAttribList = new ArrayList<Integer>();
 
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
 

		// Get the source index; (vins = 1, achats = 2)
		// Added as configuration in driver
		FileSplit fsFileSplit = (FileSplit) context.getInputSplit();
		intSrcIndex = Integer.parseInt(context.getConfiguration().get(fsFileSplit.getPath().getName()));

 
		// Initialize the list of fields to emit as output based on
		// intSrcIndex (1=Vins, 2=Achats)
		if (intSrcIndex == 1) // Vins
		{
			lstRequiredAttribList.add(1); // CRU
			lstRequiredAttribList.add(2); // MILL
			lstRequiredAttribList.add(3); // DEGRE
		} else // Achats
		{
			lstRequiredAttribList.add(3); // LIEU
 
		}
 
	}
 
	private String buildMapValue(String arrEntityAttributesList[]) {
		// This method returns csv list of values to emit based on data entity
 
		strMapValueBuilder.setLength(0);// Initialize
 
		// Build list of attributes to output based on source - vins/achats
		for (int i = 1; i < arrEntityAttributesList.length; i++) {
			// If the field is in the list of required output
			// append to stringbuilder
			if (lstRequiredAttribList.contains(i)) {
				strMapValueBuilder.append(arrEntityAttributesList[i]).append(",");
			}
		}
		if (strMapValueBuilder.length() > 0) {
			// Drop last comma
			strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
		}
 
		return strMapValueBuilder.toString();
	}
 
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		if (value.toString().length() > 0) {
			String arrEntityAttributes[] = value.toString().split(",");
 
			ckwKey.setjoinKey(arrEntityAttributes[0].toString());
			ckwKey.setsourceIndex(intSrcIndex);
			txtValue.set(buildMapValue(arrEntityAttributes));
 
			context.write(ckwKey, txtValue);
		}
 
	}
}

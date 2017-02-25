import java.io.IOException; 

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Reduce Side Join
/*
 * "immigrant" refer to persons with no U.S. citizenship at birth. This 
	population includes naturalized citizens, lawful permanent 
	residents, refugees and asylees, persons on certain temporary 
	visas, and the unauthorized.
 */

public class taxpayer 
{
	
	public static class taxMapper extends Mapper<LongWritable, Text, LongWritable, Text> //calculate tax for all record
	{	
	
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] line = value.toString().split(",");
			int age = Integer.parseInt(line[0]);
			double income = (Double.parseDouble(line[5]))*12;
			double tax = 0.0;
			String filer = line[4];
			String citizen = line[8];
			
			if(age > 14)
			{
				if((filer.contains(" Single")) || (filer.contains(" Nonfiler"))) // impose non-filer as Single and calculate tax
				{
					/*Single
					Taxable Income 		Tax Rate
					$0—$9,275 			10%
					$9,276—$37,650 		$927.50 plus 15% of the amount over $9,275
					$37,651—$91,150 	$5,183.75 plus 25% of the amount over $37,650
					$91,151—$190,150 	$18,558.75 plus 28% of the amount over $91,150
					$190,151—$ 413,350 	$46,278.75 plus 33% of the amount over $190,150
					$413,351—$415,050 	$119,934.75 plus 35% of the amount over $413,350
					$415,051 or more 	$120,529.75 plus 39.6% of the amount over $415,050*/
					if(income < 9275)
					{
						tax = income * 0.10;
					}
					else if(income > 9276 && income < 37650)
					{
						tax = ((income - 9275)*0.15) + 927.50;
					}
					else if(income > 37651 && income < 91150)
					{
						tax = ((income - 37650)*0.25) + 5183.75;
					}
					else if(income > 91151 && income < 190150)
					{
						tax = ((income - 91150)*0.28) + 18558.75;
					}
					else if(income > 190151 && income < 413350)
					{
						tax = ((income - 190150)*0.33) + 46278.75;
					}
					else if(income > 413351 && income < 415050)
					{
						tax = ((income - 413350)*0.35) + 119934.75;
					}
					else if (income > 415051)
					{
						tax = ((income - 415050)*0.396) + 120529.75;
					}		
				}
				/*Married Filing Jointly or Qualifying Widow(er)
				Taxable Income 		Tax Rate
				$0—$18,550 			10%
				$18,551—$75,300 	$1,855 plus 15% of the amount over $18,550
				$75,301—$151,900 	$10,367.50 plus 25% of the amount over $75,300
				$151,901—$231,450 	$29,517.50 plus 28% of the amount over $151,900
				$231,451—$413,350 	$51,791.50 plus 33% of the amount over $231,450
				$413,351—$466,950 	$111,818.50 plus 35% of the amount over $413,350
				$466,951 or more 	$130,578.50 plus 39.6% of the amount over $466,950*/
				else if(filer.contains(" Joint"))
				{
					if(income < 18550)
					{
						tax = income * 0.10;
					}
					else if(income > 18551 && income < 75300)
					{
						tax = ((income - 18550)*0.15) + 1855;
					}
					else if(income > 75301 && income < 151900)
					{
						tax = ((income - 75300)*0.25) + 10367.50;
					}
					else if(income > 151901 && income < 231450)
					{
						tax = ((income - 151900)*0.28) + 29517.50;
					}
					else if(income > 231451 && income < 413350)
					{
						tax = ((income - 231450)*0.33) + 51791.50;
					}
					else if(income > 413351 && income < 466950)
					{
						tax = ((income - 413350)*0.35) + 111818.50;
					}
					else if (income > 466951)
					{
						tax = ((income - 466950)*0.396) + 130578.50;
					}
				}
				/*Head of Household
				Taxable Income 		Tax Rate
				$0—$13,250 			10%
				$13,251—$50,400 	$1,325 plus 15% of the amount over $13,250
				$50,401—$130,150 	$6,897.50 plus 25% of the amount over $50,400
				$130,151—$210,800 	$26,835 plus 28% of the amount over $130,150
				$210,801—$413,350 	$49,417 plus 33% of the amount over $210,800
				$413,351—$441,000 	$116,258.50 plus 35% of the amount over $413,350
				$441,001 or more 	$125,936 plus 39.6% of the amount over $441,000*/
				else if (filer.contains(" Head"))
				{
					if(income < 13250)
					{
						tax = income * 0.10;
					}
					else if(income > 13251 && income < 50400)
					{
						tax = ((income - 13250)*0.15) + 1325;
					}
					else if(income > 50401 && income < 130150)
					{
						tax = ((income - 50401)*0.25) + 6897.50;
					}
					else if(income > 130151 && income < 210800)
					{
						tax = ((income - 130151)*0.28) + 26835;
					}
					else if(income > 210801 && income < 413350)
					{
						tax = ((income - 210800)*0.33) + 49417;
					}
					else if(income > 413351 && income < 441000)
					{
						tax = ((income - 413350)*0.35) + 116258.50;
					}
					else if (income > 441001)
					{
						tax = ((income - 441000)*0.396) + 125936;
					}
				}
				String taxcal = String.format("%.2f",tax);								
				
				context.write(new LongWritable(0),new Text("TaxAll\t"+taxcal)); //write tax for all record
				
				if(citizen.contains(" Foreign"))
				{
					context.write(new LongWritable(1), new Text("ForeignTax\t"+taxcal+","+filer)); //write tax and filerstatus only for immigrant
				}
				if(citizen.contains(" Native"))
				{
					context.write(new LongWritable(2), new Text("NativeTax\t"+taxcal+","+filer)); //write tax and filerstatus only for native
				}				
			}		
		}		
	}
	
	public static class incomeMapper extends Mapper<LongWritable, Text, LongWritable, Text> //extract income
	{
		
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] line = value.toString().split(",");
			double income = (Double.parseDouble(line[5]))*12;
			String citizen = line[8];
			String incval = String.format("%.2f", income);
			context.write(new LongWritable(3), new Text("IncomeAll\t"+incval)); //write income of all record
			if(citizen.contains(" Foreign"))
			{
				context.write(new LongWritable(4), new Text("ForeignIncome\t"+incval)); //write income of immigrant
			}
			if(citizen.contains(" Native"))
			{
				context.write(new LongWritable(5), new Text("NativeIncome\t"+incval)); //write income of native
			}			
		}			
	}
	
	
	public static class taxandmeanReducer extends Reducer<LongWritable, Text, LongWritable, Text> // find tax, mean income
	{
		public void reduce(LongWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			double taxall = 0, foreigntax = 0, nativetax = 0;
			double taxnonpayforeign = 0, taxnonpaynative = 0;
			double tempmeanall = 0, tempmeanfor = 0, tempmeannat = 0;
			int countall = 0, countfor = 0, countnat = 0;
			for (Text entry : values) 
			{
				String parts[] = entry.toString().split("\t");
				if (parts[0].equals("TaxAll")) 
				{
					taxall += Double.parseDouble(parts[1]);		//calculate total tax			
				}
				else if (parts[0].equals("ForeignTax")) 
				{
					String rows[] = parts[1].split(",");
					foreigntax += Double.parseDouble(rows[0]);	//calculate immigrant paid tax
					if(rows[1].contains(" Nonfiler"))
					{
						taxnonpayforeign += Double.parseDouble(rows[0]);	//calculate immigrant non-paid tax [assumed as "Single"]
					}
				} 
				else if (parts[0].equals("NativeTax")) 
				{
					String rows[] = parts[1].split(",");
					nativetax += Double.parseDouble(rows[0]);	//calculate native paid tax
					if(rows[1].contains(" Nonfiler"))
					{
						taxnonpaynative += Double.parseDouble(rows[0]);		//calculate native non-paid tax [assumed as "Single"]
					}
				} 
				else if (parts[0].equals("IncomeAll")) 		
				{
					countall++;
					tempmeanall += Double.parseDouble(parts[1]);	//calculate overall mean income
				}
				else if (parts[0].equals("ForeignIncome")) 
				{
					countfor++;
					tempmeanfor += Double.parseDouble(parts[1]);	//calculate immigrant's mean income
				}
				else if (parts[0].equals("NativeIncome")) 
				{
					countnat++;
					tempmeannat += Double.parseDouble(parts[1]);	//calculate native's mean income
				}				
			}
			double meanall = 0, meanfor = 0, meannat = 0;
			meanall = tempmeanall/countall;
			meanfor = tempmeanfor/countfor;
			meannat = tempmeannat/countnat;
			
			String taxsall = String.format("%.2f",taxall);
			String taxsfor = String.format("%.2f",foreigntax);
			String nonsfor = String.format("%.2f",taxnonpayforeign);
			String taxsnat = String.format("%.2f",nativetax);
			String nonsnat = String.format("%.2f",taxnonpaynative);
			String meansall = String.format("%.2f",meanall);
			String meansfor = String.format("%.2f",meanfor);
			String meansnat = String.format("%.2f",meannat);
			
			if(taxall != 0)
			{
				context.write(new LongWritable(1), new Text(" USA Total Tax:\t $ "+taxsall));
			}
			if(foreigntax != 0)
			{
				context.write(new LongWritable(2), new Text(" Total Immigrant Tax:\t  $ "+taxsfor));
			}
			if(taxnonpayforeign != 0)
			{
				context.write(new LongWritable(3), new Text(" Immigrant Nonfiled Tax: $ "+nonsfor));
			}
			if(nativetax != 0)
			{
				context.write(new LongWritable(4), new Text(" Total Native Tax:\t  $ "+taxsnat));
			}
			if(taxnonpaynative != 0)
			{
				context.write(new LongWritable(5), new Text(" Native Nonfiled Tax:\t  $ "+nonsnat));
			}			
			if(!Double.isNaN(meanall))
			{
				context.write(new LongWritable(6), new Text(" US Mean Income:\t  $ "+meansall));
			}
			if(!Double.isNaN(meanfor))
			{
				context.write(new LongWritable(7), new Text(" Immigrant's Mean Income:$ "+meansfor));
			}
			if(!Double.isNaN(meannat))
			{
				context.write(new LongWritable(8), new Text(" Native's Mean Income:\t  $ "+meansnat));
			}
			
		}
	}
	
	
	
	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
	    job.setJarByClass(taxpayer.class);
	    job.setJobName("Tax and Per Capita");
		job.setReducerClass(taxandmeanReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, taxMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, incomeMapper.class);
		
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}	
}
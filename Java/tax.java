import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//73, High school graduate, Widowed, Female, Nonfiler,1700.09, Not in universe, 
//United-States, Native- Born in the United States,0

public class tax 
{
	public static class taxMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		private LongWritable keys = new LongWritable();
		long mykey = 0;
		String record = null;
		
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException 
		{
			String[] line = value.toString().split(",");
			int age = Integer.parseInt(line[0]);
			double income = (Double.parseDouble(line[5]))*12;
			double tax = 0.0;
			String filer = line[4];
			
			if(age > 14)
			{
				if((filer.contains(" Single")) || (filer.contains(" Nonfiler")))
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
				record = line[0]+","+line[1]+","+line[2]+","+line[3]+","+line[4]+","+String.format("%.2f",income)
				+","+String.format("%.2f",tax)+","+line[6]+","+line[7]+","+line[8]+","+line[9];
				
				mykey++;
				keys.set(mykey);
				
				context.write(keys,new Text(record));
			}			
		}
	}
	
	public static void main(String[] args) throws Exception 
	{
		
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf);
	    job.setJarByClass(tax.class);
	    job.setJobName("Tax Calculation");
		job.setMapperClass(taxMapper.class);
	    job.setNumReduceTasks(0);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}	
}

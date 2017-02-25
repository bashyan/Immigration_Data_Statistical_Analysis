register '/home/bashyan-ubuntu/Documents/censusproject/datafu-1.2.0.jar';

define Median datafu.pig.stats.Median();

define BagConcat datafu.pig.bags.BagConcat();

census = load '/home/bashyan-ubuntu/Documents/censusproject/Censusdata/sampledata' 
using PigStorage(',') as (Age: int, Income:double, Citizenship:chararray);

total = foreach (group census all) generate COUNT(census) as total; ------2000----------

totimmi = filter census by (Citizenship == ' Foreign born- Not a citizen of U S ') OR (Citizenship == ' Foreign born- U S citizen by naturalization');

immigrants = foreach (group totimmi all) generate COUNT(totimmi) as immigrants; -------199------

age = load '/home/bashyan-ubuntu/Documents/censusproject/Censusdata/agegroup' using PigStorage('\t') as (Age: int, Agegroup:chararray);

joinage = join census by Age, age by Age;

joinedbag = foreach joinage generate $0,$11 as Agegroup,$1,$2,$3,$4,$5 as Income,$6,$7,$8 as Citizenship,$9;

ordercensus = order joinedbag by Income asc;

medianincome = foreach (group ordercensus all) generate FLATTEN(Median(ordercensus.Income)) as Median;

poverty = filter ordercensus by (Income<(medianincome.Median*0.60));

totalpoverty = foreach (group poverty all) generate COUNT(poverty) as tot_poverty;  ------402-------


                                                    -- POVERTY IMMIGRANT--


immipov = filter poverty by (Citizenship == ' Foreign born- Not a citizen of U S ') OR (Citizenship == ' Foreign born- U S citizen by naturalization');

immipovcount = foreach (group immipov all) generate COUNT(immipov) as immigrant_poverty;  -----48--------

immichild = filter immipov by (Agegroup == 'infants');

childpov = foreach (group immichild all) generate COUNT(immichild) as childpov;  -----------4----------- 

immiold = filter immipov by (Agegroup == 'senior citizen') or (Agegroup == 'elderly');

oldpov = foreach (group immiold all) generate COUNT(immiold) as oldpov;   ----------6-----------

joinbags = cogroup total by total, immigrants by immigrants, totalpoverty by tot_poverty, immipovcount by immigrant_poverty; 

bagcon = foreach joinbags generate BagConcat(total,immigrants,totalpoverty,immipovcount);

inters = foreach bagcon generate total.total, immigrants.immigrants, (total.total - immigrants.immigrants) as natives, totalpoverty.tot_poverty,  immipovcount.immigrant_poverty, (totalpoverty.tot_poverty - immipovcount.immigrant_poverty) as native_poverty, childpov.childpov, oldpov.oldpov;

interlimit = limit inters 1;---------(2000,199,1801,402,48,354)------------

percentage = foreach interlimit generate total, natives, 
		(((double)natives*100)/(double)total) as native_per,immigrants, 
		(((double)immigrants*100)/(double)total) as immi_per, tot_poverty, 
		(((double)tot_poverty*100)/(double)total) as poverty_per, native_poverty, 
		ROUND_TO((((double)native_poverty*100)/(double)natives),2) as nativepov_per, 
		ROUND_TO((((double)native_poverty*100)/(double)tot_poverty),2) as nativetotpov_per, immigrant_poverty, 
		ROUND_TO((((double)immigrant_poverty*100)/(double)immigrants),2) as immipoverty_per, 
		ROUND_TO((((double)immigrant_poverty*100)/(double)tot_poverty),2) as immitotpov_per,
		ROUND_TO((((double)childpov*100)/(double)immigrants),2) as immichildpov_per,
		ROUND_TO((((double)oldpov*100)/(double)immigrants),2) as immioldpov_per;

percent = foreach percentage generate CONCAT(
		'\nTotal Sample Population \t',(chararray)$0,
		'\n\nTotal Natives Percentage\t',(chararray)$2,'% of total population',
		'\nTotal Immigrants Percentage\t',(chararray)$4,'% of total population',
		'\n\nTotal Poverty Percentage\t',(chararray)$6,'% of total population', 
		'\n\nNative Poverty percentage\t',(chararray)$8,'% among natives', 
		'\nNative Poverty Percentage\t',(chararray)$9,'% among total poverty',
		'\n\nImmigrant Poverty Percent.\t',(chararray)$11,'% among immigrants', 
		'\nImmigrant Poverty Percent.\t',(chararray)$12,'% among total poverty', 
		'\n\nInfant Poverty Percentage\t',(chararray)$13,'% among immigrants', 
		'\nOldage Poverty Percentage\t',(chararray)$14,'% among immigrants\n');

store percent into '/home/hduser/Documents/PovertyPercentage';


---------(2000,1801,90.05,199,9.95,402,20.1,354,19.66,88.06,48,24.12,11.94,2.01,3.02)-----------

/*
(
Total Sample Population		2000

Total Natives Percentage	90.05% of total population
Total Immigrants Percentage	9.95% of total population

Total Poverty Percentage	20.1% of total population

Native Poverty percentage	19.66% among natives
Native Poverty Percentage	88.06% among total poverty

Immigrant Poverty Percent.	24.12% among immigrants
Immigrant Poverty Percent.	11.94% among total poverty

Infant Poverty Percentage	2.01% among immigrants
Oldage Poverty Percentage	3.02% among immigrants
)
*/






















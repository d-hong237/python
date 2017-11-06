create or replace function udf_weight_std_dev(observation varchar(2000), freq varchar(2000))
returns float immutable as $$

'''
Created on 11/6/2017 
UDF function calculates weighted standard deviation(population)
between the observation and freq variables.

Data prep needs to be performed before using this function as the variables will be passed in as a string.

Check these steps before using this UDF.
1. Get distinct values from observation variable, sum the freq variable 
and group it by the observation variable and any other dimension fields as needed. 

2. Transpose observation and freq variable to a single row using Redshift LISTAGG() function. 

See example below:  

create temp table sample
as
select b.reporting_Week_Begin_Date, a.code,
cast(a.<observation> as int) as observation, sum(a.<freq>) as freq
from table_a a
join table_b b
on a.a_day = b.b_day
and a.<freq>  > 0
group by 1, 2, 3
order by 2;
 
 
create temp table get_results
sortkey(reporting_week_Begin_date, vendor_code)
as
 
select
reporting_week_Begin_Date,
code,
udf_weight_std_dev( LISTAGG(observation, ','), LISTAGG(freq,',') ) as weighted_std_dev
from sample
group by 1, 2
order by 2;
'''

import numpy as np

def getWeightstdev(observation, freq):

   observation = [int(i) for i in observation.split(',')]
   freq = [int(i) for i in freq.split(',')]

   n = 0
   finish = len(freq) - 1

   results = []

   while n <= finish:
      results.append([observation[n]] * freq[n])
      n = n + 1
   
   flatten = [x for results in results for x in results]

   return np.std(flatten)
   
return getWeightstdev(observation, freq)

$$ LANGUAGE plpythonu;
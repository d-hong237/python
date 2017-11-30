create or replace function udf_weight_std_dev(observation varchar(3000), freq varchar(3000))
returns float immutable as $$

'''
Change Log:
11/15/17 - Allowed observation variable to be either an int or float.
         - Implemented constraint on observation to only have up to 3 decimal places.
11/29/17 - Updated example on documentation below.


Created on 11/6/2017 
UDF function calculates weighted standard deviation(population)
between the observation and freq variables.

Observation can be either a float or int.
Freq must be an int.

Data prep needs to be performed before using this function as the variables will be passed in as a string.

Check these steps before using this UDF.
1. Get distinct values from observation variable, sum the freq variable 
and group it by the any dimension fields as needed. 

2. Transpose observation and freq variable to a single row using Redshift LISTAGG() function. 

See example below:  

vendor_code  obs      freq
ABC           5         3
ABC           6         5
ABC           7         1


Obs: [5,6,7]  
Freq: [3,5,1]

Weighted Standard Deviation = [5,5,5,6,6,6,6,6,7] 
 

 
select
vendor_code,
udf_weight_std_dev( LISTAGG(Obs, ','), LISTAGG(Freq,',') ) as weighted_std_dev
from sample
group by 1;
'''

import numpy as np

def getWeightstdev(observation, freq):

   #Check to see if string contains a dot. If so, check decimal places.
   if '.' in observation:
      observation = [float(i) for i in observation.split(',')]
   
      for x in observation:
         if len(str(x).split('.')[1]) > 3:
            raise ValueError("Observation variable can only have up to 3 decimal places")
         else:
            pass
   else:
      observation = [float(i) for i in observation.split(',')]


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

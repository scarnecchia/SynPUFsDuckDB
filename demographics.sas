libname synpufs "/apps/socprojects/dmqa/users/dscarnec/SynPUFsDuckDB/data/";

proc format;
  value agecat_years
  .       = "00. Missing"
  low-<0  = "00. Negative"
  0-<2    = "01. 0-1 yrs"
  2-<5    = "02. 2-4 yrs"
  5-<10   = "03. 5-9 yrs"
  10-<15  = "04. 10-14 yrs"
  15-<19  = "05. 15-18 yrs"
  19-<22  = "06. 19-21 yrs"
  22-<25  = "07. 22-24 yrs"
  25-<35 = "08. 25-34 yrs"
  35-<45 = "09. 35-44 yrs"
  45-<55 = "10. 45-54 yrs"
  55-<60 = "11. 55-59 yrs"
  60-<65 = "12. 60-64 yrs"
  65-<70 = "13. 65-69 yrs"
  70-<75 = "14. 70-74 yrs"
  75-high = "15. 75+ yrs"
  ;
run;

proc sql noprint;
create table _dem_l2_age as
select floor((intck("month",birth_date,"31Dec2010"d)-(day("31Dec2010"d)<day(birth_date)))/12) as age_years label="Age (Years)"
     ,  sex
     ,  hispanic
     ,  race
     ,  count(*) as count format=comma16.
from synpufs.demographic
group by calculated age_years, sex, hispanic, race;
quit;

proc sql noprint;
     create table dem_l2_agecat_catvars as
     select put(age_years,agecat_years.) as agecat_years label="Age Category (Years)"
     ,  sex
     ,  hispanic
     ,  race
     ,  sum(count) as count format=comma16.
     from _dem_l2_age
     group by calculated agecat_years, sex, hispanic, race
     order by calculated agecat_years;
quit;

proc sql noprint;
     create table dem_l2_agecat as
     select agecat_years
     ,  sum(count) as count format=comma16.
     from dem_l2_agecat_catvars
     group by agecat_years;
quit;

proc print data=dem_l2_agecat_catvars; run;
proc print data=dem_l2_agecat; run;
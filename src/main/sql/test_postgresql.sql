select nys."schoolId", nys."schoolYear", avg(nys.enrolled) as average_enrolled
from ch15_nyc_schools nys 
group by (nys."schoolId", nys."schoolYear")
order by average_enrolled desc;

select count(distinct "schoolId") from ch15_nyc_schools cns;
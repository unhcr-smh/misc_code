


select
	"SiteLabel"
	, count(1) as "count_meters"
	, string_agg(distinct "Latitude"||','||"Longitude", '| ') as "unique_lat_long"
	, string_agg(distinct "Serial", '| ') as "serials"
	, count(distinct "Serial") as "count_distinct_meters"
	, count(distinct "Latitude"||','||"Longitude") as "num_lat_long_pairs"
from "meter_verbose_update_check"
group by "SiteLabel"
--having count(distinct "Latitude"||','||"Longitude") > 1
having count(1) <> count(distinct "Serial")
;

-- All sites have only one lat/long pair
-- Total count per site matches total distinct number of meters per site
-- 396 sites


select *
from "meter_verbose_update_check"
;

select 
	"SiteLabel"
	, string_agg(distinct "Latitude"||','||"Longitude", '| ') as "unique_lat_long"
	, string_agg(distinct "Serial", '| ') as "serials"
from "meter_verbose_update_check"
where "Latitude" = 0
group by "SiteLabel"
;

"00980AB3"
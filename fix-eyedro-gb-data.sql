SELECT * FROM public.newtable;
-- DROP TABLE public."00980A9C";

CREATE TABLE "0098087C" (
	"index" int8 NULL,
	"DeviceSerial" text NULL,
	"Timestamp" int8 NULL,
	"Wh" int8 NULL,
	"Timestamp_temp" timestamp NULL
);

delete from "0098087C" where "Timestamp" > 1681765200

INSERT INTO "0098087C"
("index", "DeviceSerial", "Timestamp", "Wh", "Timestamp_temp") --, "year", "month", week, day_of_month, day_of_week, "hour", "minute", "time", mean_wh, median_wh, imputed_mean, imputed_median)
VALUES(0, '0098087C', 1606551300, 0, '2020-11-28 02:15:00.000 -0600') --, 2020, 11, 48, 28, 'saturday', 8, 15, '08:15', 18374, 18374, 18374, 18374);

----delete from "00980A9C" where index != 0

select * from (select "Timestamp", count("Timestamp") as cnt from "0098087C" ac group by "Timestamp") as a  where cnt > 1

select min("Timestamp_temp") mints, max("Timestamp_temp") maxts from "0098087C"

with d as (select "Timestamp_temp" ts, "Wh" wh from "0098087C"),
d1 as (select "Timestamp_temp" ts, "Wh" wh from "0098087C"),
m as (select min("Timestamp_temp") mints, max("Timestamp_temp") maxts from "0098087C")
select d.wh, d1.wh from d,d1 where (d.ts  - '1 week'::interval) = d1.ts limit 10

select d1.ts, d1.wh, (select wh from d as d2 where (d1.ts  + '1 year'::interval) = d2.ts) from d as d1 limit 10


select ts, wh, mints, maxts, (ts  + '1 year'::interval), (select wh from d where ts = (ts  + '1 year'::interval)),
case when wh != 0 then wh
   else case when ts < '2021-06-22' then (select wh from d where ts = (ts  + '1 year'::interval))
        	 else wh 
   end
end wh
from d,m where d.ts > '2022-01-01' order by ts


select ts, wh from d where ts = (ts  + '1 year'::interval)

====================================================

-- 0098087C  3253 zeros out of 70000  after 

select count(*) from  "0098087C" where "Wh" < 1

with d as (select "Timestamp_temp" ts, "Wh" wh from "0098087C"),
d1 as (select ("Timestamp_temp" + '2 day'::interval) ts, "Wh" wh from "0098087C")
,d2 as (select ("Timestamp_temp" + '4 day'::interval) ts, "Wh" wh from "0098087C"),
w1 as (select ("Timestamp_temp" + '8 day'::interval) ts, "Wh" wh from "0098087C")
,w2 as (select ("Timestamp_temp" + '16 day'::interval) ts, "Wh" wh from "0098087C"),
xd1 as (select ("Timestamp_temp" - '2 day'::interval) ts, "Wh" wh from "0098087C")
,xd2 as (select ("Timestamp_temp" - '4 day'::interval) ts, "Wh" wh from "0098087C"),
xw1 as (select ("Timestamp_temp" - '8 day'::interval) ts, "Wh" wh from "0098087C")
,xw2 as (select ("Timestamp_temp" - '16 day'::interval) ts, "Wh" wh from "0098087C"),
--,m as (select min("Timestamp_temp") mints, max("Timestamp_temp") maxts from "0098087C")
d3 as (select d.ts, d.wh, d1.wh wh1, d2.wh wh2, w1.wh wh3, w2.wh wh4, xd1.wh wh5, xd2.wh wh6, xw1.wh wh7, xw2.wh wh8 
from d,d1,d2,w1,w2,xd1,xd2,xw1,xw2 where d.ts = d2.ts and d.ts = d1.ts and d.ts = w1.ts and d.ts = w2.ts 
and d.ts = xd1.ts and d.ts = xd2.ts and d.ts = xw1.ts and d.ts = xw2.ts),
res as (
select ts + interval '1 hr' as ts,
case when wh != 0 then wh * 4
  when wh1 > wh2 then wh1 * 4
  when wh2 > wh3 then wh2 * 4
  when wh3 > wh4 then wh3 * 4
  when wh4 > wh5 then wh4 * 4
  when wh5 > wh6 then wh5 * 4
  when wh6 > wh7 then wh6 * 4
  when wh7 > wh8 then wh7 * 4
  else wh8 * 4
end watts
from d3 order by ts desc)
--select count(*) from res where wh = 0
select * from res order by ts 

---update "00980A9C" set "Wh"=0 where "index" in (select index from "00980A9C" ae where "Wh" between 1 and 600)

0	009004E2	1606551300	0	2020-11-28 02:15:00.000 -0600	2020	11	48	28	saturday	8	15	08:15	18374	18374	18374	18374






select * from "calabar_00980A9C" cac where "Timestamp_temp"  between '2022-12-31' and '2023-01-01'
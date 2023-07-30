/* Max laps in a single race */
	SELECT
		race_driver.race_id,
		max(race_driver_laps_lap_number) as max_laps_in_race
	FROM race_driver_laps
	inner join race_driver on race_driver.race_driver_id = race_driver_laps.race_driver_id
	group by race_driver.race_id


/* Find the finals */
	with find_finals as (
		select
			race_class.race_class_id,
			race_class.race_class_name,
			heat.event_id,
			heat.heat_id,
			case when heat.heat_id = max(heat.heat_id) over (partition by heat.event_id) THEN true else false end as is_final,
			race.race_number,
			race.race_id,
			race.race_length,
			race.race_logged_class_name
		from race
		inner join race_class on race_class.race_class_id = race.race_class_id
		inner join heat on heat.heat_id = race.heat_id
		order by heat.event_id desc, race_class.race_class_id desc, heat.heat_id desc, race.race_number desc
	)
	select
	row_number() over (partition by is_final, event_id, race_class_id order by race_number desc) as speed_rank,
	*
	from find_finals where is_final = true
	order by event_id desc


/* Laps per driver in race */
	with winners as (
		SELECT
		heat.event_id,
		race_driver.race_id,
		race_driver.driver_id,
		max(race_driver_laps.race_driver_laps_lap_number) as lap_count,
		sum(race_driver_laps.race_driver_laps_time_millisecond) as total_time
		FROM
		race_driver_laps
		inner join race_driver on race_driver.race_driver_id = race_driver_laps.race_driver_id
		inner join race on race_driver.race_id = race.race_id
		inner join heat on heat.heat_id = race.heat_id
		group by heat.event_id, race_driver.race_id, race_driver.driver_id
	),
	driver_laps as (
		select
			row_number() over (partition by race_id order by lap_count desc, total_time asc) as rn,
			lap_count,
			total_time,
			total_time / lap_count as avg_lap_time,
			race_id,
			driver_id
		from winners
		order by lap_count desc, total_time asc
	)
	select * from driver_laps
	order by race_id, rn



### SQLite DBrowser Bug - leave the end of the commit close off...
with
	find_finals as (
		select
			race.race_class_id,
			heat.event_id,
			heat.heat_id,
			case when heat.heat_id = max(heat.heat_id) over (partition by heat.event_id) THEN true else false end as is_final,
			race.race_number,
			race.race_id,
			race.race_length,
			race.race_logged_class_name
		from race
		inner join race_class on race_class.race_class_id = race.race_class_id
		inner join heat on heat.heat_id = race.heat_id
		order by heat.event_id desc, race_class.race_class_id desc, heat.heat_id desc, race.race_number desc
	),
	identify_finals as (
		select
		event_id,
		race_class_id,
		row_number() over (partition by is_final, event_id, race_class_id order by race_number desc) as speed_rank,
		race_id,
		heat_id
		from find_finals where is_final = true
		order by event_id desc
	),
	driver_laps_1 as (
		SELECT
		race_driver.race_id,
		race_driver.driver_id,
		max(race_driver_laps.race_driver_laps_lap_number) as lap_count,
		sum(race_driver_laps.race_driver_laps_time_millisecond) as total_time
		FROM
		race_driver_laps
		inner join race_driver on race_driver.race_driver_id = race_driver_laps.race_driver_id
		group by race_driver.race_id, race_driver.driver_id
	),
	driver_laps_2 as (
		select
			row_number() over (partition by race_id order by lap_count desc, total_time asc) as pos,
			lap_count,
			total_time,
			total_time / lap_count as avg_lap_time,
			race_id,
			driver_id
		from driver_laps_1
		order by lap_count desc, total_time asc
	),
	driver_laps as (
		select
			*
		from driver_laps_2
		order by lap_count desc, total_time asc
	)
	select * from driver_laps
	
	
	/*
select
	event.event_date,
	race_class.race_class_name,
	identify_finals.speed_rank,
	driver_laps.pos,
	driver_laps.driver_id,
	identify_finals.*,
	driver_laps.*
from identify_finals
inner join driver_laps on driver_laps.race_id = identify_finals.race_id
inner join race_class on race_class.race_class_id = identify_finals.race_class_id
inner join event on event.event_id = identify_finals.event_id
order by event_date desc, race_class_id desc, speed_rank asc, pos asc
*/
 

with
driver_to_view as (
	select driver_id from driver where driver.driver_name = 'YOUR NAME HERE'
),
find_finals as (
	select
		race.race_class_id,
		heat.event_id,
		heat.heat_id,
		case when heat.heat_id = max(heat.heat_id) over (partition by heat.event_id) THEN true else false end as is_final,
		race.race_number,
		race.race_id,
		race.race_length,
		race.race_logged_class_name
	from race
	inner join race_class on race_class.race_class_id = race.race_class_id
	inner join heat on heat.heat_id = race.heat_id
	order by heat.event_id desc, race_class.race_class_id desc, heat.heat_id desc, race.race_number desc
),
identify_finals as (
	select
	event_id,
	race_class_id,
	row_number() over (partition by is_final, event_id, race_class_id order by race_number desc) as race_speed_rank,
	race_id,
	heat_id
	from find_finals where is_final = true
	order by event_id desc
),
interesting_race_1 as (
	select race.race_id, event_id, race_class_id, race_driver.driver_id from race_driver
	inner join race on race.race_id = race_driver.race_id
	inner join heat on heat.heat_id = race.heat_id
	where race_driver.driver_id in (select driver_id from driver_to_view)
),
interesting_race as (
	select distinct
		interesting_race_1.event_id,
		interesting_race_1.race_class_id,
		race.race_id,
		race.race_number,
		race.race_logged_class_name
	from interesting_race_1
	inner join heat on heat.event_id = interesting_race_1.event_id
	inner join race on race.heat_id = heat.heat_id
	where race.race_class_id = interesting_race_1.race_class_id
),
driver_laps_1 as (
	SELECT
		race_driver.race_id,
		race_driver.driver_id,
		max(race_driver_laps.race_driver_laps_lap_number) as lap_count,
		sum(race_driver_laps.race_driver_laps_time_millisecond) as total_time
	FROM
	race_driver_laps
	inner join race_driver on race_driver.race_driver_id = race_driver_laps.race_driver_id
	group by race_driver.race_id, race_driver.driver_id
),
driver_laps_2 as (
	select
		row_number() over (partition by race_id order by lap_count desc, total_time asc) as pos,
		lap_count,
		total_time,
		total_time / lap_count as avg_lap_time,
		race_id,
		driver_id
	from driver_laps_1
	order by lap_count desc, total_time asc
),
driver_laps_3 as (
	select
		*,
		case when pos = 1 then true else false end as is_winner,
		case when pos = (max(pos) over (partition by race_id) / 2) then true else false end as is_top_half,
		case when driver_id in (select driver_id from driver_to_view) then true else false end as is_me
	from driver_laps_2
	order by lap_count desc, total_time asc
),
driver_laps as (
	select * from driver_laps_3
	where is_top_half or is_winner or is_me
),
stats as (
	select
		event.event_id,
		event.event_date,
		race_class.race_class_name,
		race_class.race_class_id,
		identify_finals.race_speed_rank,
		driver.driver_name,
		driver.driver_id,
		driver_laps.pos,
		driver_laps.driver_id,
		total_time,
		lap_count,
		avg_lap_time,
		is_winner,
		is_top_half,
		interesting_race.race_logged_class_name,
		is_me
	from identify_finals
	inner join driver_laps on driver_laps.race_id = identify_finals.race_id
	inner join race_class on race_class.race_class_id = identify_finals.race_class_id
	inner join event on event.event_id = identify_finals.event_id
	inner join driver on driver.driver_id = driver_laps.driver_id
	inner join interesting_race on interesting_race.race_id = identify_finals.race_id
	order by event_date desc, race_class.race_class_id desc, race_speed_rank asc, pos asc
),
speed_rank_1_winner as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_winner and race_speed_rank = 1
),
speed_rank_2_winner as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_winner and race_speed_rank = 2
),
speed_rank_3_winner as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_winner and race_speed_rank = 3
),
speed_rank_1_top_half as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_top_half and race_speed_rank = 1
),
speed_rank_2_top_half as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_top_half and race_speed_rank = 2
),
speed_rank_3_top_half as (
	select
		event_id,
		race_class_id,
		avg_lap_time,
		driver_name
	from stats
	where is_top_half and race_speed_rank = 3
),
mine as (
	select
		event_id,
		event_date,
		race_speed_rank,
		race_class_id,
		race_class_name,
		avg_lap_time,
		driver_name
	from stats
	where is_me
)
select
	mine.event_date as event_date,
	mine.race_class_name as class_name,
	mine.driver_name as person_name,
	mine.race_speed_rank as person_speed_race,
	mine.avg_lap_time as person_avg_lap_time_ms,
	speed_rank_1_winner.avg_lap_time as race_1_winner_avg_lap_time_ms,
	speed_rank_1_winner.driver_name as race_1_winner_driver_name,
	speed_rank_2_winner.avg_lap_time as race_2_winner_avg_lap_time_ms,
	speed_rank_2_winner.driver_name as race_2_winner_driver_name,
	speed_rank_3_winner.avg_lap_time as race_3_winner_avg_lap_time_ms,
	speed_rank_3_winner.driver_name as race_3_winner_driver_name,
	speed_rank_1_top_half.avg_lap_time as race_1_top_half_avg_lap_time_ms,
	speed_rank_1_top_half.driver_name as race_1_top_half_driver_name,
	speed_rank_2_top_half.avg_lap_time as race_2_top_half_avg_lap_time_ms,
	speed_rank_2_top_half.driver_name as race_2_top_half_driver_name,
	speed_rank_3_top_half.avg_lap_time as race_3_top_half_avg_lap_time_ms,
	speed_rank_3_top_half.driver_name as race_3_top_half_driver_name
from mine
left join speed_rank_1_winner on speed_rank_1_winner.race_class_id = mine.race_class_id and speed_rank_1_winner.event_id = mine.event_id
left join speed_rank_2_winner on speed_rank_2_winner.race_class_id = mine.race_class_id and speed_rank_2_winner.event_id = mine.event_id
left join speed_rank_3_winner on speed_rank_3_winner.race_class_id = mine.race_class_id and speed_rank_3_winner.event_id = mine.event_id
left join speed_rank_1_top_half on speed_rank_1_top_half.race_class_id = mine.race_class_id and speed_rank_1_top_half.event_id = mine.event_id
left join speed_rank_2_top_half on speed_rank_2_top_half.race_class_id = mine.race_class_id and speed_rank_2_top_half.event_id = mine.event_id
left join speed_rank_3_top_half on speed_rank_3_top_half.race_class_id = mine.race_class_id and speed_rank_3_top_half.event_id = mine.event_id
where event_date not like '2020-01-01%' and class_name like 'YOUR_RACE_CLASS'
order by event_date desc, class_name








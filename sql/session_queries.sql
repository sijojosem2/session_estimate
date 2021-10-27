--******************************
-- Simple Version
--******************************

-- All the unsuccessful bookings per 15 min sessions are shown in the outer most select query
select *,
	max(case when event_name similar to ('%Ride Started%|%Ride Done%|%Rating Screen%')  then 'Y' else 'N' end ) over w_booking as succesful_booking
from
(
-- Session break down to 15 mins based on the previous event calculated here
select
	*,

	sum(case
			when minute_diff_between_events < 15  then 0
			else 1
		end ) over w_session_ct as sess_count

from
(
-- Inner most query to get available records, calculate cumulative running ride count per user,per device and
-- calculate minute difference between events

select
	*,
	coalesce (extract (epoch from (created_at - lag(created_at) over w_preceeding_events))::integer/60,0) as minute_diff_between_events,
	sum(case when event_name similar to ('%Ride Done%') then 1 else 0 end) over w_preceeding_events as running_ride_count
from
	mobile_events
window
	w_preceeding_events as (partition by lower(anonymous_id),lower(context_device_id) order by created_at rows between unbounded preceding and current row)
) as a
window
	w_session_ct as (partition by lower(anonymous_id),lower(context_device_id) order by created_at)


)	as b
window
w_booking as (partition by lower(anonymous_id),lower(context_device_id),sess_count order by  created_at rows between unbounded preceding and unbounded following)

order by
	LOWER(anonymous_id),
	created_at ;










--******************************
-- Extended Version
--******************************

-- This was my initial attempt to ponder upon and decipher how to break into sessions, although the below query is resource
-- intensive it gives a little bit extra information about the time interval per user from opening application until
-- booking a ride




-- The total of all calculations given here, in the outermost select
--
-- 	    * First event value and the corresponding time , within the boundary defined in the inner query to calculate
--        intervals between opening the app and starting the ride.
-- 	    * The session intervals. If there is more than a 15 min break between events then this is considered a new
--        session.

select
	anonymous_id,
	business_zoned,
	context_app_version,
	context_device_id,
	context_os_name,
	created_at,
	running_ride_count,
	sess_count,
	max(case when event_name similar to ('%Ride Started%|%Ride Done%|%Rating Screen%')  then 'Y' else 'N' end ) over w_booking as succesful_booking,
	-- The 'preceeding_event' and  'event_interval' are technically not required, it was my first attempt to decipher how to break down session by
	-- measuring the duration within opening application and ride completion. Although this is redundant, I decided to keep this as a model in case
	-- there is a use case for this in the future.
	case
		when event_name = 'Ride Started - Successful' then  lag(app_open_event)  over w_dataset
		when event_name = 'Ride Done - Successful'    then  lag(rde_open_event)  over w_dataset
		else NULL
	end as preceeding_event,
	event_name,
	case
		when event_name = 'Ride Started - Successful' then  created_at - lag(last_app_open_time)  over w_dataset
		when event_name = 'Ride Done - Successful'    then  created_at - lag(last_rde_start_time) over w_dataset
		else NULL
	end as event_interval

from
(
-- In the below we prepare the following
--
-- 	    * First event value and the corresponding time , within the boundary defined in the inner query to calculate
-- 		  intervals between opening the app and starting the ride.
-- 	    * The session intervals. If there is more than a 15 min break between events then this is considered a new
--        session.

select
	anonymous_id,
	business_zoned,
	context_app_version,
	context_device_id,
	context_os_name,
	event_name,
	created_at,
	running_ride_count,
	minute_diff,
	ride_time_calc_boundary,
	sum(case
			when minute_diff < 15  then 0
			else 1
		end ) over w_session_ct as sess_count,
	first_value (event_name) over w_app_event  as app_open_event,
	first_value (created_at) over w_app_event  as last_app_open_time,
	first_value (event_name) over w_ride_event as rde_open_event,
	first_value (created_at) over w_ride_event as last_rde_start_time
from
(

-- In this nested select we are defining boundaries to calculate duration between below two events of interest
--
-- 1. minute_diff 				- Difference in minutes between consecutive events; I will use this to break events into 15 min sessions
-- 2. running_ride_count		- Cumulative running total of daily rides
-- 3. app_open_book_boundary 	- Event marker for boundary between event 'Application Opened' until 'Ride Started - Successful'
-- 4. ride_time_calc_boundary	- Event marker for boundary between 'Ride Started - Successful' until 'Ride Done - Successful'
--
--
-- The end answers we seek for this calculation is:
--
--      * How long does it take for consecutive events leading to the booking? For the purpose of the exercise
-- 			I have broken down sessions to 15 mins each, any break more than 15 mins is another session
-- 		* How many successful rides have been booked by the user per day
-- 		* How long it takes from opening application until start ride.
-- 		* Whether each time the application is opened , does this lead to a ride booking? assuming the user opens the app multiple time a day,
-- 			we need to focus only on the sessions that lead to a successful booking, in this case I have assumed successful booking to be the event - 'Ride Started - Successful'
--

select
	*,
	coalesce (extract (epoch from (created_at - lag(created_at) over w_preceeding_events))::integer/60,0) as minute_diff,
	sum(case when event_name similar to ('%Ride Done%') 										then 1 else 0 end)     	over w_preceeding_events as running_ride_count,
	sum(case when event_name in ('Application Opened','Ride Started - Successful') 				then 1 else NULL end)  	over w_preceeding_events as app_open_book_boundary,
	sum(case when event_name in ('Ride Started - Successful','Ride Done - Successful') 			then 1 else NULL end)	over w_preceeding_events as ride_time_calc_boundary
from
	mobile_events
window
	w_preceeding_events as (partition by lower(anonymous_id),lower(context_device_id) order by created_at rows between unbounded preceding and current row)

) as boundary_definitions
window
	w_session_ct as (partition by lower(anonymous_id),lower(context_device_id) order by created_at),
	w_app_event as  (partition by lower(anonymous_id),lower(context_device_id),app_open_book_boundary order by created_at),
	w_ride_event as (partition by lower(anonymous_id),lower(context_device_id),ride_time_calc_boundary order by created_at)

) as pre_event_selection

window
 w_dataset as (partition by lower(anonymous_id),lower(context_device_id) order by created_at),
 w_booking as (partition by lower(anonymous_id),lower(context_device_id),sess_count order by  created_at rows between unbounded preceding and unbounded following)
order by
	LOWER(anonymous_id),
	created_at





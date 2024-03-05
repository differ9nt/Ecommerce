with sessions_info as (
  select
    user_pseudo_id,
    (select value.int_value from e.event_params where key = 'ga_session_id') as session_id,
    user_pseudo_id || cast((select value.int_value from e.event_params where key = 'ga_session_id') as string) as user_session_id,
    regexp_extract((select value.string_value from e.event_params where key = 'page_location'), r'(?:https:\/\/)?[^\/]+\/(.*)') as landing_page_location,
    geo.country,
    device.category as device_category,
    device.language as device_language,
    device.operating_system,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name as campaign
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where
    event_name = 'session_start'
),
events as (
  select
    timestamp_micros(event_timestamp) as event_timestamp,
    event_name,
    user_pseudo_id || cast((select value.int_value from e.event_params where key = 'ga_session_id') as string) as user_session_id
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e
  where
    event_name in ('session_start', 'view_item', 'add_to_cart', 'begin_checkout', 'add_shipping_info', 'add_payment_info',  'purchase')
)
select
  e.event_timestamp,
  s.*,
  e.event_name
from sessions_info s
left join events e using(user_session_id)
order by e.event_timestamp

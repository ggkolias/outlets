-- Test that report dates are within reasonable range
-- Dates should be between 2020-01-01 and current date + 1 day

select *
from {{ ref('daily_reporting_table') }}
where report_date < '2020-01-01'::date
   or report_date > current_date + interval '1 day'

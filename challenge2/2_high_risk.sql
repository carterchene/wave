
with 

-- DUCKDB SQL IS USED 

clean as (

  select *
  from trx -- assunming raw table is called trx
  where 1=1
  and (status <> 'Failed') -- assuming we want to include failed trx for high risk consideration
  and (customer_id is not null) 
)

,high_risk as (

  select *
  from clean
  --im assuming we want to retain all the information for each of the transactions, hence a qualify rather than a group by
  qualify row_number() over (partition by transaction_date order by amount desc) < 3 -- get the highest two transcations for each day
) 


-- im unsure if im supposed to filter out negative totals as well, so this cte does that incase im supposed to
-- select from previous cte if not
,high_risk_no_negative as (
  select * 
  from high_risk
  qualify sum(amount) over (partition by transaction_date) >= 0 

)
select * from high_risk_no_negative






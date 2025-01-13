
with 

-- DUCKDB SQL IS USED 

clean as (

  select *
  from trx -- assunming raw table is called trx
  where 1=1
  -- (status <> 'Failed') assuming we want to include failed trx for high risk consideration
  and (customer_id is not null) 
  and amount > 0 -- no negative numbers
)

,high_risk as (

  select *
  from clean
  --im assuming we want to retain all the information for each of the transactions, hence a qualify rather than a group by
  qualify row_number() over (partition by transaction_date order by amount desc) < 3 -- get the highest two transcations for each day
) 
  
select * from high_risk






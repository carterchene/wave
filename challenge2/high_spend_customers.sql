
with 

-- DUCKDB SQL IS USED 

clean as (

  select *
  from trx -- assunming raw table is called trx
  where 1=1
  and status <> 'FAILED'
  and customer_id is not null
  and transaction_date > current_date - interval 7 days -- filter to only last 7 days
)

,customer_spend_last_week as (

  select   customer_id
         , sum(amount) as week_amount
  from clean
  group by customer_id

)

,high_spenders as (
  select * 
  from customer_spend_last_week 
  where week_amount > 400 

)

select * from high_spenders 






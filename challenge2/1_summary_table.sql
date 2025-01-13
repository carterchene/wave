
with 

-- DUCKDB SQL IS USED 

-- i couldve created a dbt scaffold and pointed it to a duckdb database file but decided that would create too many extra files considering i'm explicitly asked for just 3 files

clean as (

  select *
  from trx -- assuming the raw table is called trx
  where status <> 'FAILED'
  and customer_id is not null
)

,summary as (

  select    customer_id
          , count(transaction_id) as total_transactions -- count the transactions 
          , sum(amount) as total_amount -- sum them
          , max(transaction_date) as last_transaction_date -- get the latest date
  from clean
  group by customer_id 
  having sum(amount) >= 0 -- make sure the total isnt negative
)
  
select * from summary 






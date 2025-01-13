
with 

-- DUCKDB SQL IS USED 

clean as (

  select *
  from trx -- assunming raw table is called trx
  where (status <> 'Failed')
  and (customer_id is not null) 
  and transaction_date > current_date - interval 7 days -- filter to only last 7 days
)

,high_spend_customers as (

  select *
  from clean
  qualify sum(amount) over (partition by customer_id, transaction_date) > 400 -- get all the transacations for a given customer, for a given day, that they spend more than 400
  
)
  
select * from high_spend_customers 






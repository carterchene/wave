# explanation and approach to data quality

summary table was straight forward, jsut filter and group. filter the aggregated vals via HAVING

the high risk table was a little ambiguous. i assumed we want to retain each records information (rather than some aggregation),
but then it kind of makes "total_amount is non-negative" not as sensical. i just filtered out negative amounts, rather than filter 
out negatvie sums. 

high spend customers was also fairly straightforward.

i like to use qualify statements because they are much cleaner than like, window functions subquery + where clause

For data quality: 
I prefer to write many basic CTEs that are logically grouped vs a lot of complicated logic in one large query. the first CTE does basic filtering to clean the data before
any more transformations/calcluations are made, which makes it easier to follow for others and for debugging later. 

in the case of dbt I'd also write dbt tests that automatically run to make sure the tables look like what is expected. 



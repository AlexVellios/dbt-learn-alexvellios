with orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payment') }}

),

pay_per_cust as (
select 
    orders.customer_id as customer_id,
    sum(payments.amount)/100 as amount
from orders
left join payments using (order_id)
group by customer_id
)

select * from pay_per_cust
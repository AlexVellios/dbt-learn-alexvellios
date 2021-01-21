with customers as (

    select * from {{ ref('stg_customers') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

payments as (

    select * from {{ ref('stg_payment') }}

),

customervalue as (
select 
    orders.customer_id as customer_id,
    sum(payments.amount)/100 as amount
from orders
left join payments using (order_id)
group by customer_id
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),


final_0 as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

),

final as (

    select
        final_0.customer_id,
        final_0.first_name,
        final_0.last_name,
        final_0.first_order_date,
        final_0.most_recent_order_date,
        final_0.number_of_orders,
        customervalue.amount as lifetime_value

    from final_0

    left join customervalue using (customer_id)

)

select * from final
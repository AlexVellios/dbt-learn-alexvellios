select
    ORDERID as order_id,
    ID as payment_id,
    AMOUNT as amount

from {{source('stripe','payment')}}
where STATUS = 'success'
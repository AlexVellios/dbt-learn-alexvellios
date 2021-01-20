select
    ORDERID as order_id,
    ID as payment_id,
    AMOUNT as amount

from raw.stripe.payment
where STATUS = 'success'
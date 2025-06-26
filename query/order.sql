SELECT
    -- Order information
    o.increment_id AS IncrementId,
    o.customer_email AS Email,
    o.customer_note AS CustomerNote,
    o.customer_is_guest AS CustomerIsGuest,
    o.created_at AS CreatedAt,
    o.order_currency_code AS OrderCurrencyCode,
    o.weight AS Weight,
    o.tax_amount AS TaxAmount,
    o.shipping_amount AS ShippingAmount,
    o.status AS Status,
    o.customer_email AS CustomerEmail,
    o.customer_firstname AS CustomerFirstname,
    o.customer_lastname AS CustomerLastname,

    -- Seller name (prefer store_id = 25, fallback to store_id = 0)
    COALESCE(seller_name_25.value, seller_name_0.value) AS SellerName,

    -- Billing
    a_billing.firstname AS BillingFirstname,
    a_billing.lastname AS BillingLastname,
    a_billing.telephone AS BillingTelephone,
    a_billing.street AS BillingStreet,
    a_billing.postcode AS BillingPostcode,
    a_billing.city AS BillingCity,
    a_billing.region AS BillingRegion,
    a_billing.country_id AS BillingCountryId,

    -- Shipping
    a_shipping.firstname AS ShippingFirstname,
    a_shipping.lastname AS ShippingLastname,
    a_shipping.telephone AS ShippingTelephone,
    a_shipping.street AS ShippingStreet,
    a_shipping.postcode AS ShippingPostcode,
    a_shipping.city AS ShippingCity,
    a_shipping.region AS ShippingRegion,
    a_shipping.country_id AS ShippingCountryId,

    -- Items
    i.name AS ItemName,
    i.sku AS ItemSku,
    i.qty_ordered AS ItemQtyOrdered,
    i.price AS ItemPrice,
    i.discount_amount AS ItemDiscountAmount,
    i.weight AS ItemWeight,
    i.tax_amount AS ItemTaxAmount,

    -- Payment
    p.amount_ordered AS PaymentAmountOrdered,
    o.order_currency_code AS TransactionOrderCurrencyCode,
    o.status AS TransactionStatus

FROM sales_order o
     LEFT JOIN sales_order_address a_billing
        ON a_billing.parent_id = o.entity_id AND a_billing.address_type = 'billing'
     LEFT JOIN sales_order_address a_shipping
        ON a_shipping.parent_id = o.entity_id AND a_shipping.address_type = 'shipping'
     LEFT JOIN sales_order_item i
        ON i.order_id = o.entity_id
     LEFT JOIN sales_order_payment p
        ON p.parent_id = o.entity_id
     LEFT JOIN smile_seller_entity s
        ON s.entity_id = o.seller_id

     -- Seller name: primary from store_id = 25
     LEFT JOIN (
        SELECT v.entity_id, v.value
        FROM smile_seller_entity_varchar v
        JOIN eav_attribute a ON a.attribute_id = v.attribute_id
        WHERE a.attribute_code = 'name' AND v.store_id = 25
     ) AS seller_name_25
        ON seller_name_25.entity_id = s.entity_id

     -- Fallback: store_id = 0
     LEFT JOIN (
        SELECT v.entity_id, v.value
        FROM smile_seller_entity_varchar v
        JOIN eav_attribute a ON a.attribute_id = v.attribute_id
        WHERE a.attribute_code = 'name' AND v.store_id = 0
     ) AS seller_name_0
        ON seller_name_0.entity_id = s.entity_id

WHERE o.store_id = 25;

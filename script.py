import pandas as pd
import os
import time
import zipfile
import sys
import json

# --- progress bar ---
def print_progress(current, total, bar_length=40):
    percent = float(current) / total
    arrow = '█' * int(round(percent * bar_length))
    spaces = '-' * (bar_length - len(arrow))
    sys.stdout.write(f"\rProgress: [{arrow}{spaces}] {int(percent * 100)}%")
    sys.stdout.flush()

# --- field mapping ---
order_field_mapping = [
    {'magento': 'IncrementId', 'shopify': 'Name'},
    {'magento': None, 'shopify': 'Command', 'value': 'NEW'},
    {'magento': 'Email', 'shopify': 'Email'},
    {'magento': 'CustomerNote', 'shopify': 'Note'},
    {'magento': 'CustomerIsGuest', 'shopify': 'Tags'},
    {'magento': 'CreatedAt', 'shopify': 'Processed At'},
    {'magento': 'OrderCurrencyCode', 'shopify': 'Currency'},
    {'magento': 'Weight', 'shopify': 'Weight Total'},
    {'magento': None, 'shopify': 'Tax: Included', 'value': 'TRUE'},
    {'magento': 'TaxAmount', 'shopify': 'Tax 1: Price'},
    {'magento': None, 'shopify': 'Tax 2: Title', 'value': 'Discount Tax'},
    {'magento': 'DiscountTax', 'shopify': 'Tax 2: Price'},
    {'magento': 'Status', 'shopify': 'Payment: Status'},
    {'magento': 'CustomerEmail', 'shopify': 'Customer: Email'},
    {'magento': 'CustomerFirstname', 'shopify': 'Customer: First Name'},
    {'magento': 'CustomerLastname', 'shopify': 'Customer: Last Name'},
    {'magento': 'BillingFirstname', 'shopify': 'Billing: First Name'},
    {'magento': 'BillingLastname', 'shopify': 'Billing: Last Name'},
    {'magento': 'BillingTelephone', 'shopify': 'Billing: Phone'},
    {'magento': 'BillingStreet', 'shopify': 'Billing: Address 1'},
    {'magento': 'BillingPostcode', 'shopify': 'Billing: Zip'},
    {'magento': 'BillingCity', 'shopify': 'Billing: City'},
    {'magento': 'BillingRegion', 'shopify': 'Billing: Province'},
    {'magento': 'BillingCountryId', 'shopify': 'Billing: Country Code'},
    {'magento': 'ShippingFirstname', 'shopify': 'Shipping: First Name'},
    {'magento': 'ShippingLastname', 'shopify': 'Shipping: Last Name'},
    {'magento': 'ShippingTelephone', 'shopify': 'Shipping: Phone'},
    {'magento': 'ShippingStreet', 'shopify': 'Shipping: Address 1'},
    {'magento': 'ShippingPostcode', 'shopify': 'Shipping: Zip'},
    {'magento': 'ShippingCity', 'shopify': 'Shipping: City'},
    {'magento': 'ShippingRegion', 'shopify': 'Shipping: Province'},
    {'magento': 'ShippingCountryId', 'shopify': 'Shipping: Country Code'},
    {'magento': None, 'shopify': 'Line: Type', 'value': 'Line Item'},
    {'magento': 'ItemName', 'shopify': 'Line: Title'},
    {'magento': 'ItemSku', 'shopify': 'Line: SKU'},
    {'magento': 'ItemProductOptions', 'shopify': 'Line: Properties'},
    {'magento': 'ItemQtyOrdered', 'shopify': 'Line: Quantity'},
    {'magento': 'ItemPrice', 'shopify': 'Line: Price'},
    {'magento': 'ItemDiscountAmount', 'shopify': 'Line: Discount'},
    {'magento': 'ItemWeight', 'shopify': 'Line: Grams'},
    {'magento': 'ItemTaxAmount', 'shopify': 'Line: Taxable'},
    {'magento': 'PaymentAmountOrdered', 'shopify': 'Transaction: Amount'},
    {'magento': 'TransactionOrderCurrencyCode', 'shopify': 'Transaction: Currency'},
    {'magento': 'TransactionStatus', 'shopify': 'Transaction: Status'},
    {'magento': 'SellerName', 'shopify': 'Fulfillment: Location'},
]

blank_line_fields = [
    'Line: Title', 'Line: SKU', 'Line: Properties', 'Line: Quantity', 'Line: Price', 'Line: Discount',
    'Line: Grams', 'Line: Taxable', 'Transaction: Amount', 'Transaction: Currency', 'Transaction: Status'
]

allowed_locations = {
    'Minchinbury Pickup Location', 'Stan Cash Brooklyn', 'Tottenham Pickup Location', 'Stan Cash Keilor',
    'Camberwell Pickup Location', 'Stan Cash Warehouse - NT', 'Stan Cash Warehouse - NSW',
    'Stan Cash Warehouse - QLD', 'Stan Cash Warehouse - SA', 'Pack and Send Stepney',
    'Stan Cash Warehouse - WA', 'Stan Cash Warehouse - TAS', 'Shop location'
}

giftcard_fields = [
    'giftcard_sender_name',
    'giftcard_sender_email',
    'giftcard_recipient_name',
    'giftcard_recipient_email',
    'giftcard_message',
    'giftcard_is_redeemable',
    'giftcard_created_codes'
]

def prettify_label(label):
    return label.replace('_', ' ').title()

def extract_giftcard_properties(json_str):
    try:
        data = json.loads(json_str)
        props = []
        for key in giftcard_fields:
            if key not in data:
                continue

            value = data[key]

            if not value:
                continue

            if key == 'giftcard_is_redeemable':
                if str(value).lower() == 'true':
                    props.append(f"{prettify_label(key)}: Yes")
                continue

            if isinstance(value, list):
                value = ', '.join(map(str, value))

            props.append(f"{prettify_label(key)}: {value}")

        return '\n'.join(props)
    except Exception:
        return ''

def process_orders(order_file_path, max_rows=None):
    df = pd.read_csv(order_file_path, dtype=str).fillna('')
    mapped_rows = []
    seen_orders = set()
    shipping_line_created = set()
    transaction_line_created = set()

    total = len(df)

    for idx, row in df.iterrows():
        print_progress(idx + 1, total)

        mapped_row = {}
        for field in order_field_mapping:
            magento_col = field.get('magento')
            shopify_col = field['shopify']
            value = field.get('value', '')
            split = field.get('split')

            if magento_col == 'Status' and shopify_col == 'Payment: Status':
                status_map = {
                    'pending': 'pending',
                    'afterpay_exception_review': 'pending',
                    'processing': 'paid',
                    'complete': 'paid',
                    'closed': 'refunded',
                    'canceled': 'voided',
                    'humm_processed': 'authorized',
                }
                mapped_value = status_map.get(row.get(magento_col, ''), 'unknown')

            elif shopify_col == 'Line: Taxable':
                tax = row.get('ItemTaxAmount', '0')
                mapped_value = float(tax) > 0 if tax.replace('.', '', 1).isdigit() else False

            elif shopify_col == 'Line: Properties':
                mapped_value = extract_giftcard_properties(row.get(magento_col, '{}'))

            elif magento_col == 'IncrementId' and shopify_col == 'Name':
                mapped_value = f"#{row[magento_col]}"

            elif magento_col == 'ItemDiscountAmount' and shopify_col == 'Line: Discount':
                val = row.get(magento_col, '0')
                mapped_value = str(-abs(float(val))) if val.replace('.', '', 1).isdigit() else '0'

            elif magento_col == 'CustomerIsGuest' and shopify_col == 'Tags':
                guest_flag = str(row.get(magento_col, '')).strip()
                mapped_value = 'Guest' if guest_flag == '1' else ''

            elif magento_col is None:
                mapped_value = value

            elif magento_col == 'CreatedAt' and shopify_col == 'Processed At':
                val = row.get('CreatedAt', '').strip()
                if not val or val == '0000-00-00 00:00:00':
                    mapped_value = row.get('UpdatedAt', '').strip()
                else:
                    mapped_value = val

            elif magento_col in row:
                val = row[magento_col]
                if split == 'first':
                    mapped_value = val.split(' ')[0]
                elif split == 'last':
                    mapped_value = ' '.join(val.split(' ')[1:])
                else:
                    mapped_value = val
            else:
                mapped_value = ''

            mapped_row[shopify_col] = mapped_value

        sku = mapped_row.get('Line: SKU', '')
        should_trim_sku = False

        item_product_options = row.get('ItemProductOptions', '')
        try:
            data = json.loads(item_product_options)
            options = data.get('options', [])
            if any('value' in opt and opt['value'] for opt in options):
                should_trim_sku = True
        except Exception:
            pass

        if should_trim_sku and '-' in sku:
            mapped_row['Line: SKU'] = sku.split('-')[0]
        else:
            mapped_row['Line: SKU'] = sku

        def fill_partial_fields_with_dot(mapped_row, fields):
            values = [mapped_row.get(k, '').strip() for k in fields]

            if any(values) and not all(values):
                for k in fields:
                    if not mapped_row.get(k, '').strip():
                        mapped_row[k] = '.'

        billing_fields = [
            'Billing: First Name',
            'Billing: Last Name',
            'Billing: Address 1',
            'Billing: City',
            'Billing: Country Code',
        ]

        shipping_fields = [
            'Shipping: First Name',
            'Shipping: Last Name',
            'Shipping: Address 1',
            'Shipping: City',
            'Shipping: Country Code',
        ]

        fill_partial_fields_with_dot(mapped_row, billing_fields)
        fill_partial_fields_with_dot(mapped_row, shipping_fields)

        mapped_rows.append(mapped_row)

        seller_name = row.get('SellerName', '').strip()
        order_id = row.get('IncrementId', '')

        if seller_name and order_id not in seen_orders:
            fulfillment_row = mapped_row.copy()
            status_map = {
                'complete': 'success',
                'closed': 'success',
                'canceled': 'failure',
            }
            for field in blank_line_fields:
                fulfillment_row[field] = ''
            fulfillment_row['Line: Type'] = 'Fulfillment Line'
            fulfillment_row['Fulfillment: Location'] = seller_name if seller_name in allowed_locations else 'Tottenham Pickup Location'

            raw_status = row.get('TransactionStatus', '').strip().lower()
            fulfillment_row['Fulfillment: Status'] = status_map.get(raw_status, 'unknown')

            mapped_rows.append(fulfillment_row)
            seen_orders.add(order_id)

        shipping_amount = row.get('ShippingAmount', '').strip()
        if shipping_amount and shipping_amount.replace('.', '', 1).isdigit() and order_id not in shipping_line_created:
            shipping_row = mapped_row.copy()
            for field in blank_line_fields:
                shipping_row[field] = ''
            shipping_row['Line: Type'] = 'Shipping Line'
            shipping_row['Line: Title'] = row.get('ShippingDescription', '').strip() or 'Shipping'
            shipping_row['Line: Price'] = shipping_amount
            shipping_row['Fulfillment: Location'] = ''
            mapped_rows.append(shipping_row)
            shipping_line_created.add(order_id)

        payment_amount = row.get('PaymentAmountOrdered', '').strip()
        if (
            payment_amount
            and payment_amount.replace('.', '', 1).isdigit()
            and order_id not in transaction_line_created
        ):
            transaction_row = mapped_row.copy()
            status_map = {
                'pending': 'pending',
                'afterpay_exception_review': 'pending',
                'processing': 'success',
                'complete': 'success',
                'closed': 'success',
                'canceled': 'failure',
                'humm_processed': 'success',
            }
            for field in blank_line_fields:
                transaction_row[field] = ''
            transaction_row['Line: Type'] = 'Transaction'
            transaction_row['Transaction: Amount'] = payment_amount
            transaction_row['Transaction: Currency'] = row.get('TransactionOrderCurrencyCode', '')
            
            raw_status = row.get('TransactionStatus', '').strip().lower()
            transaction_row['Transaction: Status'] = status_map.get(raw_status, 'unknown')

            transaction_row['Fulfillment: Location'] = ''
            mapped_rows.append(transaction_row)
            transaction_line_created.add(order_id)

    for row in mapped_rows:
        if row.get('Line: Type') != 'Fulfillment Line':
            row['Fulfillment: Location'] = ''
        if row.get('Line: Type') != 'Transaction':
            row['Transaction: Amount'] = ''
            row['Transaction: Currency'] = ''
            row['Transaction: Status'] = ''

    print("\n✅ Mapping complete. Writing output...")

    output_df = pd.DataFrame(mapped_rows)
    output_files = []

    if not max_rows:
        filename = 'migrated_orders.csv'
        output_df.to_csv(filename, index=False)
        output_files.append(filename)
    else:
        for i in range((len(output_df) + max_rows - 1) // max_rows):
            part_df = output_df.iloc[i * max_rows: (i + 1) * max_rows]
            filename = f'migrated_orders_part{i + 1}.csv'
            part_df.to_csv(filename, index=False)
            output_files.append(filename)

    zip_name = f'migrated_orders_{int(time.time())}.zip'
    with zipfile.ZipFile(zip_name, 'w') as zipf:
        for f in output_files:
            zipf.write(f)
            os.remove(f)

    print(f"✅ Done. Zip file saved as: {zip_name}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_orders.py <orders.csv> [max_rows|unlimited]")
        sys.exit(1)

    order_file = sys.argv[1]

    if len(sys.argv) >= 3:
        arg = sys.argv[2].lower()
        max_rows = None if arg == 'unlimited' else int(arg)
    else:
        max_rows = 900

    process_orders(order_file, max_rows)

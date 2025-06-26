import pandas as pd
import os
import time
import zipfile
import sys

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
    {'magento': 'TaxAmount', 'shopify': 'Tax: Total'},
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
    'Line: Title', 'Line: SKU', 'Line: Quantity', 'Line: Price', 'Line: Discount',
    'Line: Grams', 'Line: Taxable', 'Transaction: Amount', 'Transaction: Currency', 'Transaction: Status'
]

allowed_locations = {
    'Minchinbury Pickup Location', 'Stan Cash Brooklyn', 'Tottenham Pickup Location', 'Stan Cash Keilor',
    'Camberwell Pickup Location', 'Stan Cash Warehouse - NT', 'Stan Cash Warehouse - NSW',
    'Stan Cash Warehouse - QLD', 'Stan Cash Warehouse - SA', 'Pack and Send Stepney',
    'Stan Cash Warehouse - WA', 'Stan Cash Warehouse - TAS', 'Shop location'
}

def process_orders(order_file_path, max_rows=None):
    df = pd.read_csv(order_file_path, dtype=str).fillna('')
    mapped_rows = []
    seen_orders = set()

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

            elif magento_col == 'IncrementId' and shopify_col == 'Name':
                mapped_value = f"#{row[magento_col]}"

            elif magento_col == 'ItemDiscountAmount' and shopify_col == 'Line: Discount':
                val = row.get(magento_col, '0')
                mapped_value = str(-abs(float(val))) if val.replace('.', '', 1).isdigit() else '0'

            elif magento_col == 'CustomerIsGuest' and shopify_col == 'Tags':
                guest_flag = str(row.get(magento_col, '')).strip()
                mapped_value = 'Guest, TestStg-2' if guest_flag == '1' else 'TestStg-2'

            elif magento_col is None:
                mapped_value = value

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
        mapped_row['Line: SKU'] = sku.split('-')[0] if '-' in sku else sku

        mapped_rows.append(mapped_row)

        seller_name = row.get('SellerName', '').strip()
        order_id = row.get('IncrementId', '')
        if seller_name and order_id not in seen_orders:
            fulfillment_row = mapped_row.copy()
            for field in blank_line_fields:
                fulfillment_row[field] = ''
            fulfillment_row['Line: Type'] = 'Fulfillment Line'
            fulfillment_row['Fulfillment: Location'] = seller_name if seller_name in allowed_locations else 'Tottenham Pickup Location'
            mapped_rows.append(fulfillment_row)
            seen_orders.add(order_id)

    for row in mapped_rows:
        if row.get('Line: Type') != 'Fulfillment Line':
            row['Fulfillment: Location'] = ''

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

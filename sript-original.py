import pandas as pd
import os
import time
import zipfile
import sys

order_field_mapping = [
    {'magento': 'IncrementId', 'shopify': 'Name'},
    {'magento': None, 'shopify': 'Command', 'value': 'MERGE'},
    {'magento': 'Telephone', 'shopify': 'Phone'},
    {'magento': 'Email', 'shopify': 'Email'},
    {'magento': 'CustomerNote', 'shopify': 'Note'},
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
]

def process_orders(order_file_path):
    df = pd.read_csv(order_file_path, dtype=str).fillna('')
    mapped_df = pd.DataFrame()

    for field in order_field_mapping:
        magento_col = field.get('magento')
        shopify_col = field.get('shopify')
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
            mapped_df[shopify_col] = df[magento_col].map(status_map).fillna('unknown')

        elif shopify_col == 'Line: Taxable':
            tax_amounts = df.get('ItemTaxAmount', pd.Series('0'))
            mapped_df[shopify_col] = tax_amounts.apply(
                lambda x: float(x) > 0 if str(x).replace('.', '', 1).isdigit() else False
            )

        elif magento_col == 'IncrementId' and shopify_col == 'Name':
            mapped_df[shopify_col] = df[magento_col].apply(lambda x: f'#{x}')

        elif magento_col == 'ItemDiscountAmount' and shopify_col == 'Line: Discount':
            mapped_df[shopify_col] = df[magento_col].apply(
                lambda x: str(-abs(float(x))) if str(x).replace('.', '', 1).isdigit() else '0'
            )

        elif magento_col is None:
            mapped_df[shopify_col] = value

        elif magento_col in df.columns:
            column_data = df[magento_col]
            if split == 'first':
                mapped_df[shopify_col] = column_data.apply(lambda x: x.split(' ')[0] if isinstance(x, str) else '')
            elif split == 'last':
                mapped_df[shopify_col] = column_data.apply(lambda x: ' '.join(x.split(' ')[1:]) if isinstance(x, str) else '')
            else:
                mapped_df[shopify_col] = column_data

    # Clean SKU
    mapped_df['Line: SKU'] = mapped_df['Line: SKU'].apply(lambda x: x.split('-')[0] if isinstance(x, str) else x)

    # Chunk and export
    max_rows = 900
    total_rows = len(mapped_df)
    num_files = (total_rows // max_rows) + (1 if total_rows % max_rows != 0 else 0)
    output_files = []

    for i in range(num_files):
        start = i * max_rows
        end = start + max_rows
        part_df = mapped_df.iloc[start:end]
        filename = f'migrated_orders_part{i+1}.csv'
        part_df.to_csv(filename, index=False)
        output_files.append(filename)

    # Zip
    zip_filename = f'migrated_orders_{int(time.time())}.zip'
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file in output_files:
            zipf.write(file)
            os.unlink(file)

    print(f"✅ Done. Output ZIP: {zip_filename}")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python migrate_orders.py <orders.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    process_orders(input_file)

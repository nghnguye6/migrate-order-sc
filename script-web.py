from flask import Flask, request, send_file, render_template
import pandas as pd
import os
import tempfile
import time
import zipfile

app = Flask(__name__)

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

@app.route('/', methods=['GET'])
def index_page():
    return render_template('index.html')

@app.route('/migrate-orders', methods=['POST'])
def migrate_orders():
    if 'orders' not in request.files:
        return 'Order CSV file is required', 400

    uploaded_file = request.files['orders']
    temp_input_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
    uploaded_file.save(temp_input_file.name)

    order_input_df = pd.read_csv(temp_input_file.name, dtype=str).fillna('')

    mapped_order_df = pd.DataFrame()

    for field in order_field_mapping:
        magento_col = field.get('magento')
        shopify_col = field.get('shopify')
        value = field.get('value', '')
        split = field.get('split')

        # Special handling for 'Payment: Status' based on Magento 'Status'
        if magento_col == 'Status' and shopify_col == 'Payment: Status':
            status_map = {
                'pending': 'pending',
                'processing': 'paid',
                'complete': 'paid',
                'closed': 'refunded',
                'canceled': 'voided',
                'humm_processed': 'authorized',
            }
            mapped_order_df[shopify_col] = order_input_df[magento_col].map(status_map).fillna('unknown')

        # Set 'Line: Taxable' to True if 'ItemTaxAmount' > 0
        elif shopify_col == 'Line: Taxable':
            tax_amounts = order_input_df.get('ItemTaxAmount', pd.Series('0'))
            mapped_order_df[shopify_col] = tax_amounts.apply(lambda x: float(x) > 0 if str(x).replace('.', '', 1).isdigit() else False)

        # Add '#' before order number
        elif magento_col == 'IncrementId' and shopify_col == 'Name':
            mapped_order_df[shopify_col] = order_input_df[magento_col].apply(lambda x: f'#{x}')

        # Convert discount to negative
        elif magento_col == 'ItemDiscountAmount' and shopify_col == 'Line: Discount':
            column_data = order_input_df[magento_col]
            mapped_order_df[shopify_col] = column_data.apply(
                lambda x: str(-abs(float(x))) if str(x).replace('.', '', 1).isdigit() else '0'
            )

        # Assign fixed value if no Magento column is defined
        elif magento_col is None:
            mapped_order_df[shopify_col] = value

        # Copy value directly, with optional first/last word splitting
        elif magento_col in order_input_df.columns:
            column_data = order_input_df[magento_col]
            if split == 'first':
                mapped_order_df[shopify_col] = column_data.apply(lambda x: x.split(' ')[0] if isinstance(x, str) else '')
            elif split == 'last':
                mapped_order_df[shopify_col] = column_data.apply(lambda x: ' '.join(x.split(' ')[1:]) if isinstance(x, str) else '')
            else:
                mapped_order_df[shopify_col] = column_data

    # Clean SKUs: if SKU contains '-', keep only the first part (e.g., '834971-856891' → '834971')
    mapped_order_df['Line: SKU'] = mapped_order_df['Line: SKU'].apply(
        lambda x: x.split('-')[0] if isinstance(x, str) else x
    )

    # Post-process rows with composite SKUs (e.g., '834971-856891')
    # expanded_rows = []

    # for _, row in mapped_order_df.iterrows():
    #     sku = row.get('Line: SKU', '')
    #     if '-' in sku:
    #         parts = sku.split('-')
    #         # Original row: keep first part of SKU
    #         row['Line: SKU'] = parts[0]
    #         expanded_rows.append(row)

    #         # Duplicate row with second part of SKU and clear specific fields
    #         new_row = row.copy()
    #         new_row['Line: SKU'] = parts[1]
    #         new_row['Line: Title'] = ''
    #         new_row['Line: Price'] = ''
    #         new_row['Line: Discount'] = ''
    #         new_row['Line: Grams'] = ''
    #         expanded_rows.append(new_row)
    #     else:
    #         expanded_rows.append(row)

    # Replace the original dataframe with the expanded one
    # mapped_order_df = pd.DataFrame(expanded_rows)

    # Split into chunks of 900 rows
    max_rows_per_file = 900
    total_rows = len(mapped_order_df)
    num_files = (total_rows // max_rows_per_file) + (1 if total_rows % max_rows_per_file != 0 else 0)

    output_files = []
    for i in range(num_files):
        start = i * max_rows_per_file
        end = start + max_rows_per_file
        part_df = mapped_order_df.iloc[start:end]
        part_filename = f'migrated_orders_part{i + 1}.csv'
        part_df.to_csv(part_filename, index=False)
        output_files.append(part_filename)

    # Zip all files
    zip_filename = f'migrated_orders_{int(time.time())}.zip'
    with zipfile.ZipFile(zip_filename, 'w') as zipf:
        for file in output_files:
            zipf.write(file)

    os.unlink(temp_input_file.name)

    response = send_file(zip_filename, as_attachment=True)

    @response.call_on_close
    def cleanup():
        os.unlink(zip_filename)
        for file in output_files:
            os.unlink(file)

    return response

if __name__ == '__main__':
    app.run(port=3000, debug=True)
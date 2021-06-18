PackAvroSchema = [
    {'name': 'pack_ref', 'type': 'string'},
    {'name': 'weight', 'type': 'float'},
    {'name': 'volume', 'type': 'float'},
    {'name': 'taxable_weight', 'type': 'float'},
]

AddressV2AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'address', 'type': 'string'},
    {'name': 'zip', 'type': 'string'},
    {'name': 'city', 'type': 'string'},
    {'name': 'state', 'type': ['null', 'string']},
    {'name': 'country_iso2code', 'type': 'string'},
    {'name': 'country_iso3code', 'type': 'string'},
    {'name': 'deliver_to', 'type': ['null', 'string']},
    {'name': 'state_iso2code', 'type': ['null', 'string']},
    {'name': 'address_errors', 'type': {
        "type": "map", "values": ['string', 'string']}},
    {'name': 'normalization_errors', 'type': {
        "type": "map", "values": ['string', 'string']}},
    {'name': 'is_address_normalized', 'type': 'boolean'},
    {'name': 'is_normalization_accepted', 'type': ['null', 'boolean']},
    {'name': 'address_normalized', 'type': 'string'},
    {'name': 'zip_normalized', 'type': 'string'},
    {'name': 'city_normalized', 'type': 'string'},
    {'name': 'state_normalized', 'type': ['null', 'string']},
    {'name': 'state_iso2code_normalized', 'type': ['null', 'string']},
    {'name': 'country_iso2code_normalized', 'type': 'string'},
    {'name': 'changed_fields', 'type': {
        "type": "map", "values": ['string', 'string']}}, ]

ShippingPriceAvroSchema = [
    {'name': 'address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV2AvroSchema}},
    {'name': 'currency', 'type': 'string'},
    {'name': 'price_base', 'type': 'double'},
    {'name': 'is_preselected_service_level', 'type': 'boolean'},
    {'name': 'shipping_service_level', 'type': 'string'},
    {'name': 'contract_start_date', 'type': 'string'},
    {'name': 'contract_end_date', 'type': 'string'},
    {'name': 'transport_lead_min_time', 'type': 'double'},  # in days
    {'name': 'transport_lead_max_time', 'type': 'double'}  # in days
]

StockStatusV6AvroSchema = [
    {'name': 'missing', 'type': 'boolean'},
    {'name': 'not_received', 'type': 'boolean'},
    {'name': 'available', 'type': 'boolean'},
    {'name': 'missing_qnt', 'type': 'int'}, ]

PackDescriptionV6AvroSchema = [
    {'name': 'code', 'type': 'string'},
    {'name': 'name', 'type': 'string'}]

DeliveryDocumentV6AvroSchema = [
    {'name': 'name', 'type': 'string'},
    {'name': 'path', 'type': ['null', 'string']},
    {'name': 'time_to_process', 'type': 'double'},
    {'name': 'responsible', 'type': 'string'},
    {'name': 'auto_generated', 'type': 'boolean'},
    {'name': 'display_name', 'type': 'string'}, ]

CollectionV6AvroSchema = [
    {'name': 'name', 'type': 'string'},
    {'name': 'delivery_date', 'type': 'string'}]

CarrierServiceTypeV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'string'},
    {'name': 'code', 'type': 'string'},
    {'name': 'express', 'type': 'boolean'}]

CarrierV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'string'},
    {'name': 'email', 'type': 'string'},
    {'name': 'phone', 'type': 'string'},
    {'name': 'code', 'type': 'string'},
    {'name': 'logo_url', 'type': ['null', 'string']},
    {'name': 'website_url', 'type': ['null', 'string']},
    {'name': 'slugs', 'type': ['null', 'string'], 'default': None}]

BrandV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'string'},
    {'name': 'email', 'type': 'string'},
    {'name': 'phone', 'type': 'string'},
    {'name': 'extra_info', 'type': ['null', 'string']},
    {'name': 'website_url', 'type': ['null', 'string']},
    {'name': 'logo_url', 'type': ['null', 'string']},
    {'name': 'is_saft_compliant', 'type': 'boolean'},
    {'name': 'rex_number', 'type': ['null', 'string']},
    {'name': 'default_currency', 'type': ['null', 'string']},
    {'name': 'eori', 'type': ['null', 'string']},
    {'name': 'is_make_to_order_brand', 'type': 'boolean'},
    {'name': 'address', 'type': ['null', 'string']},
    {'name': 'tier', 'type': 'int'},
    {'name': 'is_merchandise_eu_b2b', 'type': ['null', 'boolean']},
    {'name': 'is_merchandise_extra_eu_b2b', 'type': ['null', 'boolean']},
    {'name': 'is_merchandise_eu_b2c', 'type': ['null', 'boolean']},
    {'name': 'is_merchandise_extra_eu_b2c', 'type': ['null', 'boolean']},
]

AddressV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'address', 'type': 'string'},
    {'name': 'zip', 'type': 'string'},
    {'name': 'city', 'type': 'string'},
    {'name': 'state', 'type': ['null', 'string']},
    {'name': 'country_iso2code', 'type': 'string'},
    {'name': 'country_iso3code', 'type': 'string'},
    {'name': 'deliver_to', 'type': ['null', 'string']},
    {'name': 'state_iso2code', 'type': ['null', 'string']},
    {'name': 'address_errors', 'type': {
        "type": "map", "values": ['string', 'string']}},
    {'name': 'normalization_errors', 'type': {
        "type": "map", "values": ['string', 'string']}},
    {'name': 'is_address_normalized', 'type': 'boolean'},
    {'name': 'is_normalization_accepted', 'type': ['null', 'boolean']},
    {'name': 'address_normalized', 'type': 'string'},
    {'name': 'zip_normalized', 'type': 'string'},
    {'name': 'city_normalized', 'type': 'string'},
    {'name': 'state_normalized', 'type': ['null', 'string']},
    {'name': 'state_iso2code_normalized', 'type': ['null', 'string']},
    {'name': 'country_iso2code_normalized', 'type': 'string'},
    {'name': 'changed_fields', 'type': {
        "type": "map", "values": ['string', 'string']}}, ]

CustomerV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'name', 'type': 'string'},
    {'name': 'email', 'type': 'string'},
    {'name': 'phone', 'type': 'string'},
    {'name': 'notes', 'type': ['null', 'string']},
    {'name': 'contact_person', 'type': ['null', 'string']},
    {'name': 'tax_number', 'type': ['null', 'string']},
    {'name': 'addresses', 'type': {
        'type': "array",
        "items": {
            'name': 'address',
            'type': 'record',
            'fields': AddressV6AvroSchema}}}]

ShippingCostForecastV6AvroSchema = [
    {'name': 'rank', 'type': 'int'},
    {'name': 'address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}},
    {'name': 'currency', 'type': 'string'},
    {'name': 'margin_percentage', 'type': 'double'},
    {'name': 'estimated_cost_base', 'type': 'double'},
    {'name': 'estimated_cost_vat', 'type': 'double'},
    {'name': 'estimated_cost_total', 'type': 'double'},
    {'name': 'price_charged_base', 'type': 'double'},
    {'name': 'price_charged_vat', 'type': 'double'},
    {'name': 'price_charged_total', 'type': 'double'},
    {'name': 'shipping_service_level', 'type': 'string'},
    {'name': 'incoterm', 'type': 'string'},
    {'name': 'estimated_packs', 'type': {
        'type': "array",
        "items":
            {'name': 'pack_description',
             'type': 'record',
             'fields': PackDescriptionV6AvroSchema}}},
    {'name': 'transport_lead_min_time', 'type': 'double'},
    {'name': 'transport_lead_max_time', 'type': 'double'}, ]

CarrierShippingCostV6AvroSchema = [
    {'name': 'origin_address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}},
    {'name': 'destination_address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}},
    {'name': 'currency', 'type': 'string'},
    {'name': 'estimated_cost_base', 'type': 'double'},
    {'name': 'estimated_cost_vat', 'type': 'double'},
    {'name': 'estimated_cost_total', 'type': 'double'},
    {'name': 'shipping_service_level', 'type': 'string'},
    {'name': 'incoterm', 'type': 'string'},
    {'name': 'carrier', 'type': {
        'name': 'carrier',
        'type': 'record',
        'fields': CarrierV6AvroSchema}},
    {'name': 'carrier_service_type', 'type': {
        'name': 'carrier_service_type',
        'type': 'record',
        'fields': CarrierServiceTypeV6AvroSchema}},
    {'name': 'transport_lead_time', 'type': 'double'}]  # in days

CustomerShippingCostV6AvroSchema = [
    {'name': 'currency', 'type': 'string'},
    {'name': 'margin_percentage', 'type': 'double'},
    # price_charged_total * (1 + margin_percentage)
    {'name': 'price_base', 'type': 'double'},
    {'name': 'price_vat', 'type': 'double'},
    {'name': 'price_total', 'type': 'double'},  # price_base + price_vat
    {'name': 'price_locked', 'type': 'boolean'}, ]

FulfillmentCenterV6AvroSchema = [
    {'name': 'warehouse_id', 'type': 'int'},
    {'name': 'name', 'type': 'string'},
    {'name': 'phone', 'type': 'string'},
    {'name': 'email', 'type': 'string'},
    {'name': 'address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}}]

SalesOrderV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'sales_channel_id', 'type': 'int'},
    {'name': 'sales_channel_name', 'type': 'string'},
    {'name': 'sales_channel_type', 'type': 'string'},
    {'name': 'internal_order_num', 'type': 'string'},
    {'name': 'order_num', 'type': 'string'},
    {'name': 'order_date', 'type': 'string'},
    {'name': 'currency_id', 'type': 'int'},
    {'name': 'currency', 'type': 'string'},
    {'name': 'price_base', 'type': 'double'},
    {'name': 'price_vat', 'type': 'double'},
    {'name': 'price_total', 'type': 'double'},
    {'name': 'status_id', 'type': 'int'},
    {'name': 'priority', 'type': 'boolean'},
    {'name': 'shipping_service_level', 'type': 'string'},
    {'name': 'fulfilment_type', 'type': 'string'},
    {'name': 'is_gift', 'type': 'boolean'},
    {'name': 'generate_return_label', 'type': 'boolean'},
    {'name': 'print_return_label', 'type': 'boolean'}]

VariantV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'product_id', 'type': 'int'},
    {'name': 'product_name', 'type': 'string'},
    {'name': 'reference', 'type': 'string'},
    {'name': 'model_name', 'type': 'string'},
    {'name': 'model_material', 'type': 'string'},
    {'name': 'gender', 'type': 'string'},
    {'name': 'agegroup', 'type': 'string'},
    {'name': 'ean', 'type': 'string'},
    {'name': 'hscode', 'type': 'string'},
    {'name': 'currency', 'type': 'string'},
    {'name': 'price_retail', 'type': 'double'},
    {'name': 'price_wholesale', 'type': 'double'},
    {'name': 'huubclient_id', 'type': 'int'},
    {'name': 'huubclient', 'type': 'string'},
    {'name': 'supplier', 'type': 'string'},
    {'name': 'country_of_origin', 'type': 'string'},
    {'name': 'season', 'type': 'string'},
    {'name': 'product_type', 'type': 'string'},
    {'name': 'product_family', 'type': 'string'},
    {'name': 'product_subfamily', 'type': 'string'},
    {'name': 'color', 'type': 'string'},
    {'name': 'size', 'type': 'string'},
    {'name': 'fabric', 'type': 'string'},
    {'name': 'style', 'type': 'string'},
    {'name': 'handling', 'type': 'string'},
    {'name': 'weight', 'type': 'string'},
    {'name': 'volume', 'type': 'string'},
    {'name': 'width', 'type': 'string'},
    {'name': 'height', 'type': 'string'},
    {'name': 'depth', 'type': 'string'},
    {'name': 'collection', 'type': {
        'name': 'collection',
        'type': 'record',
        'fields': CollectionV6AvroSchema}}]

DeliveryItemV6AvroSchema = [
    {'name': 'variant', 'type': {
        'name': 'variant',
        'type': 'record',
        'fields': VariantV6AvroSchema}},
    {'name': 'qnt', 'type': 'int'},
    {'name': 'original_qnt', 'type': 'int'},
    {'name': 'currency', 'type': 'string'},
    {'name': 'price_base', 'type': 'double'},
    {'name': 'price_vat', 'type': 'double'},
    {'name': 'price_total', 'type': 'double'},
    {'name': 'is_gift', 'type': 'boolean'},
    {'name': 'stock_status', 'type': {
        'name': 'StockStatusV6',
        'type': 'record',
        'fields': StockStatusV6AvroSchema}}, ]

PackV6AvroSchema = [
    {'name': 'id', 'type': 'int'},
    {'name': 'logisticunit_id', 'type': 'int'},
    {'name': 'locations_id', 'type': 'int'},
    {'name': 'activity', 'type': 'boolean'},
    {'name': 'ean', 'type': 'string'},
    {'name': 'items', 'type': {
        'type': "array",
        "items": {
            'name': 'pack_item',
            'type': 'record',
            'fields': DeliveryItemV6AvroSchema}}}]

EstimatedShippingPriceAvroSchema = [
    {'name': 'brand_fee', 'type': 'string'},
    {'name': 'shipping_prices', 'type': {
        'type': 'array',
        "items":
            {'name': 'ShippingPrice',
             'type': 'record',
             'fields': ShippingPriceAvroSchema}}},
    {'name': 'estimated_packs', 'type': {
        'type': 'array',
        "items":
            {'name': 'EstimatedPacks',
             'type': 'record',
             'fields': PackAvroSchema}}}
]

DeliveryEventV6AvroSchema = [
    {'name': 'timestamp', 'type': 'long'},
    {'name': 'id', 'type': 'string'},
    {'name': 'delivery_num', 'type': 'string'},
    {'name': 'order', 'type': {
        'name': 'order',
        'type': 'record',
        'fields': SalesOrderV6AvroSchema}},
    {'name': 'items', 'type': {
        'type': "array",
        "items":
            {'name': 'pack',
             'type': 'record',
             'fields': DeliveryItemV6AvroSchema}}},
    {'name': 'status', 'type': 'string'},
    {'name': 'tax_and_duties', 'type': 'string'},
    {'name': 'customer', 'type': {
        'name': 'customer',
        'type': 'record',
        'fields': CustomerV6AvroSchema}},
    {'name': 'brand', 'type': {
        'name': 'brand',
        'type': 'record',
        'fields': BrandV6AvroSchema}},
    {'name': 'brand_delivery_date', 'type': 'string'},
    {'name': 'shipping_costs_forecast', 'type': {
        'name': 'shipping_costs',
        'type': 'array',
        "items":
            {'name': 'shipping_costs',
             'type': 'record',
             'fields': ShippingCostForecastV6AvroSchema}}},
    {'name': 'documents', 'type': {
        'type': "array",
        "items":
            {'name': 'document',
             'type': 'record',
             'fields': DeliveryDocumentV6AvroSchema}}},
    {'name': 'packs', 'type': {
        'type': "array",
        "items":
            {'name': 'pack',
             'type': 'record',
             'fields': PackV6AvroSchema}}},
    {'name': 'transport_lead_time', 'type': 'double'},
    {'name': 'fulfillment_sla', 'type': 'double'},
    {'name': 'internal_docs_upload_time', 'type': 'double'},
    {'name': 'estimated_total_weight', 'type': [
        'null', 'double']},
    {'name': 'estimated_total_volume', 'type': [
        'null', 'double']},
    {'name': 'waybill', 'type': ['null', 'string']},
    {'name': 'home_delivery', 'type': 'boolean', 'default': True},
    {'name': 'shipping_address', 'type': ['null', {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}]},
    {'name': 'billing_address', 'type': {
        'name': 'address',
        'type': 'record',
        'fields': AddressV6AvroSchema}},
    {'name': 'label_created_date', 'type': ['null', 'string']},
    {'name': 'ship_date', 'type': ['null', 'string']},
    {'name': 'customer_shipping_cost', 'type': ['null', {
        'name': 'address',
        'type': 'record',
        'fields': CustomerShippingCostV6AvroSchema}]},
    {'name': 'brand_can_be_processed_date', 'type': ['null', 'string']},
    {'name': 'huub_can_be_processed_date', 'type': ['null', 'string']},
    {'name': 'total_weight', 'type': 'double'},
    {'name': 'total_volume', 'type': 'double'},
    {'name': 'preselected_fulfillment_center', 'type': ['null', {
        'name': 'fulfillment_center',
        'type': 'record',
        'fields': FulfillmentCenterV6AvroSchema}]},
    {'name': 'selected_fulfillment_center', 'type': ['null', {
        'name': 'fulfillment_center',
        'type': 'record',
        'fields': FulfillmentCenterV6AvroSchema}]},
    {'name': 'delivery_value_base', 'type': 'double'},
    {'name': 'delivery_value_vat', 'type': 'double'},
    {'name': 'delivery_value_total', 'type': 'double'},
    {'name': 'expected_delivery_date', 'type': ['null', 'string']},
    {'name': 'ship_limit_date', 'type': ['null', 'string']},
    {'name': 'warehouse_ship_limit_date', 'type': ['null', 'string']},
    {'name': 'estimated_delivery_date', 'type': ['null', 'string']},
    {'name': 'estimated_ship_date', 'type': ['null', 'string']},
    {'name': 'brand_can_be_processed_limit_date', 'type': ['null', 'string']},
    {'name': 'huub_can_be_processed_limit_date', 'type': ['null', 'string']},
    {'name': 'brand_documents_missing', 'type': 'boolean'},
    {'name': 'huub_documents_missing', 'type': 'boolean'},
    {'name': 'stock_status', 'type': {
        'name': 'stock_status',
        'type': 'record',
        'fields': StockStatusV6AvroSchema}},
    {'name': 'all_stock_status_calculated', 'type': ['null', 'string']},
    {'name': 'optimized', 'type': 'boolean'},
    {'name': 'process_denied', 'type': 'boolean'},
    {'name': 'owner', 'type': 'string'},
    {'name': 'process_on_stock_available', 'type': 'boolean'},
    {'name': 'brand_notifications', 'type': 'boolean'},
    {'name': 'ok_to_process', 'type': 'boolean'},
    {'name': 'forecasted_shipping_cost', 'type': ['null', {
        'name': 'ShippingCostForecast',
        'type': 'record',
        'fields': ShippingCostForecastV6AvroSchema}]},
    {'name': 'carrier_shipping_cost', 'type': ['null', {
        'name': 'CarrierShippingCost',
        'type': 'record',
        'fields': CarrierShippingCostV6AvroSchema}]},
    {'name': 'stock_allocation_channel', 'type': 'string'},
    {'name': 'incoterm', 'type': 'string'},
    {'name': 'total_taxable_weight', 'type': 'float'},
    {'name': 'shipping_price_selected', 'type': ['null', {
        'name': 'ShippingPriceSelected',
        'type': 'record',
        'fields': ShippingPriceAvroSchema}]},
    {'name': 'estimated_shipping_prices', 'type': ['null', {
        'name': 'EstimatedShippingPrice',
        'type': 'record',
        'fields': EstimatedShippingPriceAvroSchema}]},
    {'name': 'estimated_shipping_prices_failed', 'type': ['null', 'boolean']}
]

DeliveryEventsSchema = {
    'name': 'delivery_events',
    'type': 'record',
    'fields': DeliveryEventV6AvroSchema
}


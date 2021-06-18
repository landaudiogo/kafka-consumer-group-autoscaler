# Delivery Schema
```yml
DeliveryEventV6AvroSchema:
	SalesOrderV6AvroSchema: 'com.huub.types.sales_order'
	DeliveryItemV6AvroSchema: 'com.huub.types.delivery_item'
		VariantV6AvroSchema: 'com.huub.types.variant'
			CollectionV6AvroSchema: 'com.huub.types.collection'
		StockStatusV6AvroSchema: 'com.huub.types.stock_status'
	CustomerV6AvroSchema: 'com.huub.types.customer'
		AddressV6AvroSchema: 'com.huub.types.address'
	BrandV6AvroSchema: 'com.huub.types.brand'
	ShippingCostForecastV6AvroSchema: 'com.huub.typesshipping_cost_forecast'
		AddressV6AvroSchema: 'com.huub.types.address'
		PackDescriptionV6AvroSchema: 'com.huub.types.pack'
	DeliveryDocumentV6AvroSchema: 'com.huub.types.delivery_document'
		PackV6AvroSchema: 'com.huub.types.pack'
			DeliveryItemV6AvroSchema: 'com.huub.types.delivery_item'
	PackV6AvroSchema: 'com.huub.types.pack'
	AddressV6AvroSchema: 'com.huub.types.address'
	AddressV6AvroSchema: 'com.huub.types.address'
	CustomerShippingCostV6AvroSchema: 'com.huub.types.customer_shipping_cost'
	FulfillmentCenterV6AvroSchema: 'com.huub.types.fulfillment_center'
		AddressV6AvroSchema: 'com.huub.types.address'
	FulfillmentCenterV6AvroSchema: 'com.huub.types.fulfillment_center'
		AddressV6AvroSchema: 'com.huub.types.address'
	StockStatusV6AvroSchema: 'com.huub.types.stock_status'
	ShippingCostForecastV6AvroSchema: 'com.huub.typesshipping_cost_forecast'
		AddressV6AvroSchema: 'com.huub.types.address'
		PackDescriptionV6AvroSchema: 'com.huub.types.pack'
	CarrierShippingCostV6AvroSchema: 'com.huub.types.carrier_shipping_cost'
		AddressV6AvroSchema: 'com.huub.types.address'
		AddressV6AvroSchema: 'com.huub.types.address'
		CarrierV6AvroSchema: 'com.huub.types.carrier'
		CarrierServiceTypeV6AvroSchema: 'com.huub.types.carrier_service'
	ShippingPriceAvroSchema: 
		AddressV2AvroSchema
	EstimatedShippingPriceAvroSchema
		ShippingPriceAvroSchema
		PackAvroSchema
		
		
	
```
# Overview

This function is responsible for creating a batchlist for each bigquery table while inspecting the rows within the consumers.

## Variables

`table_batchlist` - A map that has as key the name of the bigquery table, and value the respective BatchList which is intended for this table.

`bytes_batches` - Every time a row is inserted into a batch, it is removed from the row_list, and the amount of bytes are added to this variable. No more rows are added to a batch if this variable is bigger than `BATCH_BYTES`.

## High Level Explanation

While the amount of bytes batched is not bigger than the limit, and there are still rows to be processed, then remove a row from the row_list and it to it's respective batchlist.

## Pseudo-code

```python
def process(row_list):
	table_batchlist, bytes_batched = 0
	while(len(row_list) and (bytes_batched < BATCH_BYTES)): 
		row = row_list.pop(0)
		batchlist = table.batchlist.get(row.table)
		batchlist.add_row(row)
		bytes_batched += row.payload_size
	return list(table_batchlist.values())
```
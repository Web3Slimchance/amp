# Schema
Auto-generated file. See `to_markdown` in `crates/core/datasets-raw/src/schema.rs`.

## blocks
````
+---------------+-------------------------------+-------------+
| column_name   | data_type                     | is_nullable |
+---------------+-------------------------------+-------------+
| _block_num    | UInt64                        | NO          |
| _ts           | Timestamp(Nanosecond, +00:00) | NO          |
| block_num     | UInt64                        | NO          |
| timestamp     | Timestamp(Nanosecond, +00:00) | NO          |
| hash          | FixedSizeBinary(32)           | NO          |
| parent_hash   | FixedSizeBinary(32)           | YES         |
| merkle_root   | FixedSizeBinary(32)           | NO          |
| nonce         | UInt32                        | NO          |
| bits          | UInt32                        | NO          |
| difficulty    | Float64                       | NO          |
| version       | Int32                         | NO          |
| size          | UInt32                        | NO          |
| stripped_size | UInt32                        | NO          |
| weight        | UInt32                        | NO          |
| tx_count      | UInt32                        | NO          |
| median_time   | Timestamp(Nanosecond, +00:00) | NO          |
+---------------+-------------------------------+-------------+
````
## transactions
````
+-------------+-------------------------------+-------------+
| column_name | data_type                     | is_nullable |
+-------------+-------------------------------+-------------+
| _block_num  | UInt64                        | NO          |
| _ts         | Timestamp(Nanosecond, +00:00) | NO          |
| block_num   | UInt64                        | NO          |
| timestamp   | Timestamp(Nanosecond, +00:00) | NO          |
| block_hash  | FixedSizeBinary(32)           | NO          |
| tx_index    | UInt32                        | NO          |
| txid        | FixedSizeBinary(32)           | NO          |
| hash        | FixedSizeBinary(32)           | NO          |
| version     | Int32                         | NO          |
| size        | UInt32                        | NO          |
| vsize       | UInt32                        | NO          |
| weight      | UInt32                        | NO          |
| locktime    | UInt32                        | NO          |
| is_coinbase | Boolean                       | NO          |
+-------------+-------------------------------+-------------+
````
## inputs
````
+-------------+-------------------------------+-------------+
| column_name | data_type                     | is_nullable |
+-------------+-------------------------------+-------------+
| _block_num  | UInt64                        | NO          |
| _ts         | Timestamp(Nanosecond, +00:00) | NO          |
| block_num   | UInt64                        | NO          |
| timestamp   | Timestamp(Nanosecond, +00:00) | NO          |
| block_hash  | FixedSizeBinary(32)           | NO          |
| tx_index    | UInt32                        | NO          |
| txid        | FixedSizeBinary(32)           | NO          |
| input_index | UInt32                        | NO          |
| spent_txid  | FixedSizeBinary(32)           | YES         |
| spent_vout  | UInt32                        | YES         |
| coinbase    | Binary                        | YES         |
| script_sig  | Binary                        | NO          |
| sequence    | UInt32                        | NO          |
| witness     | List(Binary)                  | YES         |
+-------------+-------------------------------+-------------+
````
## outputs
````
+-----------------------+-------------------------------+-------------+
| column_name           | data_type                     | is_nullable |
+-----------------------+-------------------------------+-------------+
| _block_num            | UInt64                        | NO          |
| _ts                   | Timestamp(Nanosecond, +00:00) | NO          |
| block_num             | UInt64                        | NO          |
| timestamp             | Timestamp(Nanosecond, +00:00) | NO          |
| block_hash            | FixedSizeBinary(32)           | NO          |
| tx_index              | UInt32                        | NO          |
| txid                  | FixedSizeBinary(32)           | NO          |
| output_index          | UInt32                        | NO          |
| value_sat             | Int64                         | NO          |
| script_pubkey         | Binary                        | NO          |
| script_pubkey_type    | Utf8                          | NO          |
| script_pubkey_address | Utf8                          | YES         |
+-----------------------+-------------------------------+-------------+
````

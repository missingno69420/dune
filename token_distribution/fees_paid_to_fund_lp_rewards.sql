-- https://dune.com/queries/5267659
WITH v1_gysr_fee as (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
    WHERE contract_address = 0x9b2f4eb5dd0e6e2087be155ba191da54cf1ae446
      AND topic0 = 0x6ded982279c8387ad8a63e73385031a3807c1862e633f06e09d11bcb6e282f60
      AND tx_hash = 0x51a78f506242e3808031234b81cd95ac042d1d791f9d48eb2481ab64c543abff
),
v2_gysr_fee as (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
    WHERE contract_address = 0xA8f103eEcfb619358C35F98c9372B31c64d3f4A1
      AND topic0 = 0x6ded982279c8387ad8a63e73385031a3807c1862e633f06e09d11bcb6e282f60
      AND tx_hash = 0x0e7fa04f11df392bbf832056a4f70dd210b625267f9c9ce3715edd7edf7ce8d7
),
merkl_fee_1 as (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
        WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        AND tx_hash = 0x8fc2a05ee85aca2c5b811a5c4fae238a7489211090257d8d99fc61a8eb3712cd
        AND index = 242
),
merkl_fee_2 as (
    SELECT
        date_trunc('day', block_time) AS day,
        varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS fee_amount
    FROM ethereum.logs
        WHERE topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
        AND tx_hash = 0x4785ef911e59c8fd894b176a398e3bac80bd1a1e98c1184e534a0a6285b977f8
        AND index = 643
)
select day, 'Gysr' as payee, fee_amount / 1e18 as fee_amount from v1_gysr_fee
union all
select day, 'Gysr' as payee, fee_amount / 1e18 as fee_amount from v2_gysr_fee
union all
select day, 'Merkl' as payee, fee_amount / 1e18 as fee_amount from merkl_fee_1
union all
select day, 'Merkl' as payee, fee_amount / 1e18 as fee_amount from merkl_fee_2

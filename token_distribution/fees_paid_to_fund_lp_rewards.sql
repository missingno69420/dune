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
)
select day, 'Gysr' as payee, fee_amount / 1e18 as fee_amount from v1_gysr_fee
union all
select day, 'Gysr' as payee, fee_amount / 1e18 as fee_amount from v2_gysr_fee

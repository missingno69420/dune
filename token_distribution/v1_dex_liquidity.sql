select
    "value",
    evt_block_date
from erc20_ethereum.evt_transfer
where contract_address = 0xe3dbc4f88eaa632ddf9708732e2832eeaa6688ab
and evt_block_number = 19223362
and "from" = 0x114Beeb4015735D3Ed5e100E8fbe067aaf6aF663

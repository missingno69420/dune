-- https://dune.com/queries/5169304
SELECT * FROM (
  VALUES
    (0x89c39001e3b23d2092bd998b62f07b523d23deb55e1627048b4ed47a4a38d5cc, 'Qwen QwQ 32b'),
    (0xa473c70e9d7c872ac948d20546bc79db55fa64ca325a4b229aaffddb7f86aae0, 'WAI SDXL (NSFW)'),
    (0x6cb3eed9fe3f32da1910825b98bd49d537912c99410e7a35f30add137fd3b64c, 'M8B-uncensored')
) AS t (model_id, model_name)

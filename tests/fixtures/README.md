# Test fixture provenance

These files are small deterministic inputs required by unit tests. They were copied
byte-for-byte from the DGX working checkout at `/raid/ssinha97/E4` on 2026-11-02.
They contain a policy document, a synthetic legacy operator source, and two
content-addressed library snapshot documents. No model-call logs, model responses,
private labels, telemetry data, or generated weights are included.

`protocol_v6/source_policy.json` is the baseline policy used by the protocol
migration regression test. SHA-256: `115edcde76e806d153b309264258fb950fe8054a145e3dceddd719d989609734`.

`legacy_library/` contains the two historical library snapshots used by the
exact legacy operator repair test, `CURRENT`, and the synthetic proposal source.
Hashes:

- `001619fad35ab46fd5de3372b1a0b79f0bd0ff9a513196930dfd2e1c18f9ed2c.json`: `c5417058ff54cdf3a14b728d97ae748c87884d89a0ed33ad3a9a202d7ce2ece5`
- `CURRENT`: `7959188bc599b1c308310d9f4cf6dd595078c0403eef45126d3a6ed2fd01e511`
- `e354b7b64c34c450a227fa7e6765e90b108d360af6218b18e345cb09b79efe20.json`: `2b66b5ca1c551e54c7c9a5d8b61eb61c70045190c425ff92ef040ef79bc84e07`
- `proposed_source.py`: `6434d4312587714d9146019b1c3961fde47602c5a04f85f86646eac31e60a679`

The prompt-budget support in `tests/_support/prompt_budget.py` is extracted from
`artifacts/seed_variability/launch/build_prompt_budget_candidates.py`. Tests that
need recorded model-call logs are optional integration checks and skip when the
ignored seed43 run is absent; synthetic round-trip and prompt serialization tests
run without that external run.

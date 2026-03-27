# coresight_publisher

Backend operator tooling for Coresight product publishing/removal.

- Manages product metadata in STAC
- Manages product files in Google Cloud Storage
- Provides CLI/Python workflows (not website frontend code)

## Quick Start (Delete)
```powershell
python cli.py remove -c <collection_id> -a <asset_name>
```

If STAC item ID differs from asset filename, pass it explicitly:

```powershell
python cli.py remove -c <collection_id> -a <asset_name> -i <item_id>
```

Example:

```powershell
python cli.py remove -c fems-ice-tracking -a 20260131T211624DT2353_012713_ft2_disp_20260201_210933.pmtiles
```

## Deletion Workflow
1. Validate both records exist:
   - STAC item
   - bucket asset
2. Back up STAC item in memory
3. Delete STAC item first
4. Pause for manual confirmation to verify website impact
5. If confirmed, delete bucket asset
6. If not confirmed, restore STAC item from backup
7. Append deletion record to `deleted_products_Hudson_Bay_2024.csv`

## Notes
- Run in a terminal session where `GOOGLE_APPLICATION_CREDENTIALS` is set.
- Successful deletions are logged to `deleted_products_Hudson_Bay_2024.csv`.

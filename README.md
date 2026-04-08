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

## Quick Start (Batch Delete)
```powershell
python cli.py remove-batch -c <collection_id> -p <asset_regex>
```

The batch command matches asset filenames using a Python regex.

Example:

```powershell
python cli.py remove-batch -c fems-ice-tracking -p "20260131" 
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
7. Append deletion record to tracking CSV

## Batch Deletion Workflow
1. List asset filenames from the collection bucket and match them against the provided regex
2. Print the matched asset and STAC item pairs for operator review
3. Validate every matched product exists in both places:
   - STAC item
   - bucket asset
4. Back up every matched STAC item in memory before deleting anything
5. Delete all matching STAC items first
6. Pause once for manual confirmation to verify the correct products disappeared from Coresight
7. If confirmed, delete all matching bucket assets
8. If not confirmed, restore all matched STAC items from backup
9. Append a deletion record for each deleted product to tracking CSV


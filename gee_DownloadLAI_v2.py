"""
Seasonal MODIS Leaf Area Index (LAI) for the United States (TIGER/2018/States),
exported as tiled GeoTIFFs. Same framework as the Finland script:
  * pixel-aligned tiles in an equal-area CRS, tiles outside the area skipped
  * throttled submission (MAX_ACTIVE) with automatic retry of failed tasks
  * JSON log so the script can be re-run to resume

Each REGION below gets its own CRS and its own set of tiles. The default is the
contiguous US (CONUS) in NAD83 / Conus Albers (EPSG:5070). Alaska is included
but commented out because it needs a different projection (EPSG:3338).
"""

import datetime
import json
import math
import os
import time
from collections import deque

import ee

PROJECT = "earthengine-leafareaindex"   # change to the Cloud project you want to run under
try:
    ee.Initialize(project=PROJECT)
except Exception:
    ee.Authenticate()
    ee.Initialize(project=PROJECT)

# ----------------------------------------------------------------------------
# REQUIRED INPUTS
# ----------------------------------------------------------------------------
STATES = ee.FeatureCollection("TIGER/2018/States")
NON_CONUS = ["AK", "HI", "PR", "VI", "GU", "MP", "AS"]

# name -> features to process and the (metric, equal-area) CRS to export in
REGIONS = {
    "CONUS": {
        "fc": STATES.filter(ee.Filter.inList("STUSPS", NON_CONUS).Not()),
        "crs": "EPSG:5070",   # NAD83 / Conus Albers
    },
    # "Alaska": {
    #     "fc": STATES.filter(ee.Filter.eq("STUSPS", "AK")),
    #     "crs": "EPSG:3338",   # NAD83 / Alaska Albers
    # },
}

FOCAL_STATS = [500, 1000]          # 500 = native MODIS resolution
YEARS = ["2005", "2006"]           # extend to e.g. [str(y) for y in range(2005, 2024)]

# MCD15A3H = 4-day Terra+Aqua composite (already contains Terra, so never merge
# with MOD15A2H). Used whenever a year is covered by more than one collection.
MODIS_COLLECTIONS = {
    "MODIS/061/MCD15A3H": range(2004, datetime.date.today().year + 1),
    "MODIS/061/MOD15A2H": range(2002, datetime.date.today().year + 1),
}
PREFERRED_COLLECTION = "MODIS/061/MCD15A3H"

REQUIRE_CLEAR_SKY = False   # also require CloudState == 0 (leaves more gaps)
NODATA_VALUE = None         # e.g. -9999 to write a nodata value instead of masked pixels

# ----------------------------------------------------------------------------
# EXPORT / QUEUE SETTINGS
# ----------------------------------------------------------------------------
EXPORT_FOLDER = "US_LAI"
PIXEL_M = 500
TILE_PX = 2500          # tile edge in 500 m pixels (1250 km). CONUS ~ 4x3 tiles.
STACK_SEASONS = False   # True = one 4-band file per (focal, year): 4x fewer tasks
MAX_ACTIVE = 10         # tasks allowed in READY/RUNNING at once
POLL_SECONDS = 20
LOG_FILE = "submitted_tasks_us.json"   # separate from the Finland log

MAX_RETRIES = 3                          # resubmissions per tile after the first attempt
RETRY_STATES = {"FAILED", "CANCELLED"}   # note: a task you cancel by hand is also retried
NON_RETRYABLE = ("memory", "too many pixels", "invalid", "not found",
                 "permission denied", "syntax")

SEASONS = [("01-01", "03-31"), ("04-01", "06-30"), ("07-01", "09-30"), ("10-01", "12-31")]


# ----------------------------------------------------------------------------
# LAI PROCESSING
# ----------------------------------------------------------------------------
def pick_collection(year):
    candidates = [k for k, yrs in MODIS_COLLECTIONS.items() if year in yrs]
    if not candidates:
        raise ValueError(f"No MODIS LAI collection covers year {year}")
    return PREFERRED_COLLECTION if PREFERRED_COLLECTION in candidates else candidates[0]


def prep_lai(img):
    """Mask fill values / low-quality retrievals, apply 0.1 scale, rename to LAI."""
    lai = img.select("Lai")
    qc = img.select("FparLai_QC")

    valid = lai.lte(100)                                   # 249-255 are fill values
    main_method = qc.rightShift(5).bitwiseAnd(7).lte(1)    # SCF_QC: main algorithm only
    mask = valid.And(main_method)

    if REQUIRE_CLEAR_SKY:
        mask = mask.And(qc.rightShift(3).bitwiseAnd(3).eq(0))  # CloudState == 0

    return (
        lai.updateMask(mask)
        .multiply(0.1)
        .rename("LAI")
        .copyProperties(img, ["system:time_start"])
    )


def border_mask(fc):
    """Rasterize the (detailed) TIGER borders at export resolution.

    Much cheaper than image.clip() with the full-resolution state polygons.
    """
    return (
        fc.map(lambda f: ee.Feature(f).set("m", 1))
        .reduceToImage(["m"], ee.Reducer.first())
        .rename("m")
    )


def season_image(year, season_idx, focal_m, geom, mask):
    """Seasonal mean LAI, optionally smoothed with a circular focal mean."""
    start = f"{year}-{SEASONS[season_idx][0]}"
    end = f"{year}-{SEASONS[season_idx][1]}"
    # filterDate end is exclusive; the original script behaved the same way.
    coll = (
        ee.ImageCollection(pick_collection(int(year)))
        .filterDate(start, end)
        .filterBounds(geom)
        .select(["Lai", "FparLai_QC"])
        .map(prep_lai)
    )
    comp = coll.mean().rename("LAI")
    if focal_m != 500:
        comp = comp.reduceNeighborhood(
            reducer=ee.Reducer.mean(),
            kernel=ee.Kernel.circle(radius=focal_m, units="meters"),
        ).rename("LAI")
    # Mask to the border once on the image (not per tile) so tile edges have no seams.
    out = comp.updateMask(mask).toFloat()
    if NODATA_VALUE is not None:
        out = out.unmask(NODATA_VALUE)
    return out, start


# ----------------------------------------------------------------------------
# TILING
# ----------------------------------------------------------------------------
def build_tiles(geom, crs, tile_px):
    """Pixel-aligned grid (in CRS meters) over the geometry; drops tiles outside it."""
    ring = geom.transform(crs, 1000).bounds().coordinates().get(0).getInfo()
    xs, ys = [c[0] for c in ring], [c[1] for c in ring]
    x0 = math.floor(min(xs) / PIXEL_M) * PIXEL_M     # snap origin to the 500 m grid
    y1 = math.ceil(max(ys) / PIXEL_M) * PIXEL_M
    step = tile_px * PIXEL_M
    nx = math.ceil((max(xs) - x0) / step)
    ny = math.ceil((y1 - min(ys)) / step)

    inset = 0.25 * PIXEL_M  # keeps Earth Engine from adding an extra edge pixel

    def rect(west, north):
        return ee.Geometry.Rectangle(
            [west + inset, north - step + inset, west + step - inset, north - inset],
            proj=crs,
            geodesic=False,
        )

    origins = {}
    for r in range(ny):
        for c in range(nx):
            origins[f"r{r}c{c}"] = (x0 + c * step, y1 - r * step)

    feats = [ee.Feature(rect(w, n), {"tid": tid}) for tid, (w, n) in origins.items()]
    keep = ee.FeatureCollection(feats).filterBounds(geom).aggregate_array("tid").getInfo()

    return {
        tid: {
            "region": rect(*origins[tid]),
            "crs": crs,
            "transform": [PIXEL_M, 0, origins[tid][0], 0, -PIXEL_M, origins[tid][1]],
        }
        for tid in keep
    }


# ----------------------------------------------------------------------------
# SUBMISSION WITH THROTTLING, RETRIES AND RESUME LOG
# ----------------------------------------------------------------------------
def load_log():
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE) as f:
            return json.load(f)
    return {}


def save_log(log):
    with open(LOG_FILE, "w") as f:
        json.dump(log, f, indent=2)


def already_done(task_id):
    try:
        state = ee.data.getTaskStatus(task_id)[0]["state"]
    except Exception:
        return False
    return state in {"READY", "RUNNING", "COMPLETED", "SUCCEEDED"}


def submit(name, img, tile):
    task = ee.batch.Export.image.toDrive(
        image=img,
        description=name,
        folder=EXPORT_FOLDER,
        region=tile["region"],
        crs=tile["crs"],
        crsTransform=tile["transform"],
        maxPixels=1e13,
        fileFormat="GeoTIFF",
    )
    task.start()
    return task


def run_queue(work, log):
    """Submit (name, image, tile) jobs with throttling; resubmit retryable failures."""
    pending = deque((n, i, t, 0) for n, i, t in work)   # last field = attempts so far
    active, failed = {}, []

    while pending or active:
        while pending and len(active) < MAX_ACTIVE:
            name, img, tile, attempts = pending.popleft()
            task = submit(name, img, tile)
            active[name] = (task, img, tile, attempts + 1)
            log[name] = task.id
            save_log(log)
            print(f"submitted {name} (attempt {attempts + 1}, {len(active)} active)")

        time.sleep(POLL_SECONDS)

        for name in list(active):
            task, img, tile, attempts = active[name]
            st = task.status()
            state = st["state"]
            if state in {"READY", "RUNNING", "CANCEL_REQUESTED", "UNSUBMITTED"}:
                continue
            del active[name]
            if state == "COMPLETED":
                print(f"completed {name}")
                continue

            msg = st.get("error_message", "")
            retry = (
                state in RETRY_STATES
                and attempts <= MAX_RETRIES
                and not any(p in msg.lower() for p in NON_RETRYABLE)
            )
            print(f"{state}: {name} (attempt {attempts}) {msg!r} -> "
                  f"{'will resubmit' if retry else 'giving up'}")
            if retry:
                pending.append((name, img, tile, attempts))   # back of the line
            else:
                failed.append((name, state, msg))
    return failed


def main():
    jobs = []   # (name, image, tile)
    for region_name, cfg in REGIONS.items():
        fc, crs = cfg["fc"], cfg["crs"]
        geom = fc.geometry(maxError=1000)   # simplified; only for bounds/tile selection
        mask = border_mask(fc)
        tiles = build_tiles(geom, crs, TILE_PX)
        print(f"{region_name}: {len(tiles)} tiles in {crs}: {sorted(tiles)}")

        for fs in FOCAL_STATS:
            for yr in YEARS:
                if STACK_SEASONS:
                    bands = []
                    for s in range(4):
                        img, start = season_image(yr, s, fs, geom, mask)
                        bands.append(img.rename(f"LAI_{start}"))
                    items = [(f"{region_name}_{fs}_{yr}_stack", ee.Image.cat(bands))]
                else:
                    items = []
                    for s in range(4):
                        img, start = season_image(yr, s, fs, geom, mask)
                        items.append((f"{region_name}_{fs}_{start}", img))
                for desc, img in items:
                    for tid, tile in tiles.items():
                        jobs.append((f"{desc}_{tid}", img, tile))

    # Skip tiles already queued/finished from a previous run of this script
    log, work, skipped = load_log(), [], 0
    for name, img, tile in jobs:
        if name in log and already_done(log[name]):
            skipped += 1
        else:
            work.append((name, img, tile))
    print(f"{len(work)} tasks to submit, {skipped} skipped (already queued or finished)")

    failed = run_queue(work, log)

    if failed:
        print("\nGave up on:")
        for name, state, msg in failed:
            print(f"  {name}: {state} {msg}")
    print("Done.")


if __name__ == "__main__":
    main()

# Exposure Calculator Plan

## Goal

Automate the selection of exposure time, removing this task from the observing scientist. Two operating modes:

- **Offline**: Before the night starts, estimate exposure times from survey catalog data and instrument parameters, and pre-fill a schedule.
- **Online**: During observation, when astra encounters an action with `exptime: "auto"`, take one or more test exposures, analyse them, iterate until a good exposure is found, rewrite the schedule, and proceed with science frames.

---

## Definition of a Good Exposure

A good exposure satisfies three criteria:

1. **No saturation** — no pixels at or above the saturation ADU value, excluding known hot pixels and cosmic rays.
2. **Sky/source dominated** — the fundamental noise (photon noise from sources and sky background) dominates over instrumental noise (RON, dark current).
3. **Tracking quality** — exposure duration is achievable without significant trailing given the mount's tracking performance.

### Noise model

All quantities in electrons (e−):

```
N_inst      = sqrt(RON^2 + I_dark * t)
N_fundamental = sqrt(I_sky + I_src)

Requirement: N_inst << N_fundamental
```

Where:
- `I_sky`: sky background in e−/pixel (measured from image or estimated)
- `I_src`: source signal in e−/pixel (not directly known; estimated from catalogs or plate-solving)
- `RON`: read-out noise in e− (camera characterisation, constant)
- `I_dark`: dark current in e−/pixel/s (camera characterisation)

User input: acceptable SNR degradation fraction from instrumental noise (e.g. 10% → N_inst < 0.1 · N_fundamental).

### Per-frame output (future)

- Sky background estimate: median ADU across frame, or map
- Bortle class estimate (if sky background is calibrated)
- SNR estimate per pixel:
  ```
  SNR = I_src / sqrt(N_inst^2 + N_fundamental^2)
  I_src = I_tot - I_dark*t - I_sky_est
  N = sqrt(I_tot + RON^2)
  ```
  Saturated pixels: SNR forced to zero.
- Throughput estimate (if plate solution + catalog magnitudes + known instrument zero-point)

### Required instrument inputs

- `gain` — e−/ADU
- `ron` — e− RMS
- `dark_current` — e−/pixel/s
- `saturation_adu` — ADU at clipping
- `pixel_scale` — arcsec/pixel

These will be added as a first-class section of the camera configuration.

---

## Operating Modes

### Mode 1: Offline (pre-observation planning)

A CLI tool (or library function) that takes a schedule file and returns a new schedule with `exptime: "auto"` entries filled in using catalog estimates.

- Uses a `CatalogImageSource` that synthesises what a sky field would look like given Gaia photometry and the camera model.
- The same `ExposureOptimizer` loop runs on these synthetic images.
- Output: a new schedule with concrete exposure times. The original is archived.
- Optionally, `"auto"` entries can be left in place so Mode 2 refines them on sky.

### Mode 2: Online (autonomous adjustment during observation)

When astra's main loop encounters an `object` action with `exptime: "auto"`:

1. Determine an initial exposure time (from an inline hint or a default heuristic).
2. Call `ExposureOptimizer.optimize()` with an `AlpacaImageSource` (the real camera).
3. Receive the recommended exposure time.
4. Archive the current schedule.
5. Rewrite the schedule with the concrete exposure time.
6. Proceed with the science sequence using the determined exposure time.

Astra's main loop is the master. The optimizer is called synchronously, blocking until convergence or `max_iterations` is reached. This mirrors how `autofocus_sequence()` calls `autofocuser.run()`.

### Exptime sentinel syntax

```jsonl
{"action_type": "object", "exptime": "auto", ...}          # optimizer picks initial guess
{"action_type": "object", "exptime": "auto:60", ...}       # optimizer starts from 60s
```

The inline hint allows Mode 1's catalog estimate to serve as Mode 2's starting point. The two modes compose as pipeline stages.

---

## Architecture

### Design principles

- **Decoupling**: all logic lives in `scratch/exposure_calculator/` (later to be promoted to `src/astra/exposure_calculator/`). The only coupling to the rest of astra is:
  - `schedule_adapter.py` imports `astra.scheduler.Schedule`
  - `observatory.py` will have a thin wrapper (~10 lines) analogous to `autofocus_sequence()`
- **SOLID**: see CLAUDE.md
- **TDD**: all components except the hardware adapter are testable offline

### Module layout

```
exposure_calculator/
├── models.py           # CameraParams, ExposureAssessment (pure data, no deps)
├── interfaces.py       # ImageSource, CatalogSource (abstract base classes)
├── analyzer.py         # analyze_image(image, exptime, params) -> ExposureAssessment
├── optimizer.py        # ExposureOptimizer, _suggest_next_exptime()
├── schedule_adapter.py # has_auto_exptime(), rewrite_exptime(), archive_schedule()
└── tests/
    ├── conftest.py     # fixtures: camera params, synthetic images, real FITS hook
    ├── test_analyzer.py
    ├── test_optimizer.py
    └── test_schedule_adapter.py
```

Future additions (not yet scaffolded):
- `catalog_image_source.py` — offline mode: synthesise images from Gaia + camera model
- `alpaca_image_source.py` — online mode: take exposures via Alpaca camera device

### Key seam: `ImageSource`

```python
class ImageSource(ABC):
    def capture(self, exptime: float) -> np.ndarray: ...
```

Everything above this interface (`analyzer`, `optimizer`, `schedule_adapter`) is testable offline with synthetic numpy arrays or recorded FITS files. Everything below it (`AlpacaImageSource`) requires live hardware.

### Data flow

```
[Schedule with exptime="auto"]
        |
        v
ExposureOptimizer.optimize()
    |-- ImageSource.capture(exptime)  <- real camera (online) or synthesised (offline)
    |-- analyze_image(image, exptime, params) -> ExposureAssessment
    |-- _suggest_next_exptime(exptime, assessment) -> float
    |-- repeat until is_good or max_iterations
        |
        v
archive_schedule(schedule, archive_dir)   # original preserved
rewrite_exptime(schedule, index, exptime) # new schedule, immutable
        |
        v
[Schedule with concrete exptime]
        |
        v
astra continues with image_sequence()
```

### Integration point in astra

A thin method in `observatory.py`, analogous to `autofocus_sequence()`:

```python
def calibrate_exposure_sequence(self, action, paired_devices):
    optimizer = ExposureOptimizer(
        image_source=AlpacaImageSource(paired_devices.camera),
        camera_params=CameraParams.from_config(paired_devices),
        initial_exptime=parse_auto_exptime(action),
    )
    exptime = optimizer.optimize()
    archive_schedule(self.schedule, self.config.paths.schedule_archive)
    self.schedule = rewrite_exptime(self.schedule, action_index, exptime)
```

This method is called from `run_action()` when `action.action_type == "object"` and `has_auto_exptime(action)`.

---

## TDD Strategy

All components are tested in order of increasing dependency on external state:

| Component | Test approach | Hardware needed? |
|---|---|---|
| `CameraParams`, `ExposureAssessment` | Construct directly | No |
| `analyze_image` | Inject synthetic numpy arrays | No |
| `_suggest_next_exptime` | Pure function unit test | No |
| `ExposureOptimizer` | Inject stub `ImageSource` | No |
| `schedule_adapter` | Build `Schedule` in memory | No |
| `CatalogImageSource` | Mock Gaia responses | No |
| `AlpacaImageSource` | Integration test | Yes |

Real example FITS files from the telescope are used as fixtures for smoke-testing `analyze_image` without live hardware. Set `EXAMPLE_FITS_PATH=/path/to/image.fits` to enable these tests.

---

## What is NOT yet decided

- Exact criteria for `_suggest_next_exptime`: binary search, linear scaling, or something smarter
- How to handle the case where no valid exposure time exists within hardware limits (sky too bright / too faint)
- Schema change for `ObjectActionConfig.exptime` to accept `Union[float, str]`
- Whether `auto_expose` becomes a separate action type or remains a flag on `object` actions
- Number of exposures (`n`) optimisation (for total SNR target)

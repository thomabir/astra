# Claude Code Guidelines for Astra

## Active work

The current development task is an **exposure calculator** for automated exposure time selection. See `PLAN.md` for the full design. Work-in-progress code lives in `scratch/exposure_calculator/`.

---

## Design philosophy

### Decouple first

New functionality goes in its own module. Coupling to the rest of astra should be minimal and explicit — ideally limited to one thin integration point (analogous to how `autofocus.py` is self-contained and `observatory.py` calls it in ~10 lines).

Ask: can this module be read, tested, and reasoned about without opening `observatory.py`?

### SOLID

- **S — Single responsibility**: each class/function has one reason to change. `analyze_image` analyses images. `ExposureOptimizer` orchestrates iteration. `schedule_adapter` handles schedule mutation. These are separate.
- **O — Open/closed**: extend behaviour by adding new implementations of abstract interfaces (`ImageSource`, `CatalogSource`), not by modifying existing classes.
- **L — Liskov substitution**: a `MockImageSource` used in tests must be substitutable for an `AlpacaImageSource` in production. Design abstractions so this holds.
- **I — Interface segregation**: keep abstract interfaces narrow. `ImageSource` has one method. Don't add unrelated methods just because they're convenient.
- **D — Dependency inversion**: high-level logic (`ExposureOptimizer`) depends on abstractions (`ImageSource`), not on concrete hardware drivers. Hardware adapters are injected, not instantiated internally.

### Architecture before implementation

Agree on module boundaries, data flow, and interfaces before writing logic. The question "what are the seams?" comes before "how does the algorithm work?". This keeps the code testable and the design reviewable.

### TDD offline

All logic should be testable without a live telescope. The main tool for this is dependency injection via abstract interfaces. If a function cannot be tested with synthetic data or recorded images, the design probably needs a seam introduced.

The workflow:
1. Write the test (red)
2. Write the minimal implementation to pass (green)
3. Refactor

Use `EXAMPLE_FITS_PATH=/path/to/image.fits` to run smoke tests against real recorded exposures.

### Immutability for data transformations

Functions that transform schedules return new objects; they do not mutate in place. This makes tests simple (`assert original_schedule[0].exptime == "auto"` after a rewrite) and avoids hard-to-trace bugs.

---

## Existing patterns to follow

Before adding something new, check whether it fits an established pattern in the codebase:

- **New automation logic**: follow `autofocus.py` — self-contained module, thin wrapper in `observatory.py`, separate action type in the schedule.
- **Iterative exposure adjustment**: follow `flats_sequence()` in `observatory.py` — measure, scale, repeat.
- **Action configuration**: follow `BaseActionConfig` dataclasses in `action_configs.py`.
- **Schedule file format**: JSONL, read/written via `Schedule.from_file()` / `Schedule.save_to_jsonl()`.

---

## Running the scratch tests

```bash
uv run pytest scratch/exposure_calculator/tests/ -v
```

Expected state while in development: analyzer and optimizer tests are red (`NotImplementedError`), schedule_adapter and `has_auto_exptime` tests are green.

To run against a real FITS file:
```bash
EXAMPLE_FITS_PATH=/path/to/image.fits uv run pytest scratch/exposure_calculator/tests/ -v
```

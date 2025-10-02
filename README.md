# Big Data Project 2025

Tilen Ožbot, Maks Popović

## Note for viewing

We used ITables to display dataframes (for interactive filtering/pagination),
which unfortunately requires the notebook to be trusted if you use Jupyter Lab to view it
(Ctrl+Shift+C -> Trust Notebook).

Entrypoints:

- Task 1: `/notebooks/T1.ipynb`
- Task 2: `/notebooks/T2.ipynb`
- Task 3: `/notebooks/T3.ipynb`
- Task 4: `/notebooks/T4.ipynb`
- Task 6: `/notebooks/T6/readme.md`,
- Task 8: `/notebooks/T4.ipynb`
- Task 9: `/notebooks/T2.ipynb`, `/notebooks/T4.ipynb`

## Setup

Create a `/.envrc.local` and set `SYNCSCRIPT_RSYNC_REMOTE` (see `/.envrc.local.example`),
or use another way to set the environment variable for the sync script.

Sync to arnes using `./up.sh`, and download whatever was changed in the arnes copy with `./down.sh`.
Adjust `/.rsync-filter` if necessary.

Use the VSCode extension and connect to arnes, open the project directory.
Do all your work there. Use `./down.sh` (on your local machine of course) to fetch your changes
before committing them to git as usual.

## Data

All external paths are contained in the `/ext` directory, which is in `/.gitignore` and `.rsync-filter`.
Run `./ext.sh` to set up symlinks to point to the correct paths in arnes, adjust them if necessary.
Everything else in the project expects this directory to exist and be properly set up,
it won't happen automatically.

- `/ext/src` points to a directory containing all the .parquet source files.
- `/ext/out` points to a directory written to by the outputs of all notebooks.
  For example, `/ext/out/t1/yellow.parquet` would contain the full repartitioned yellow dataset from task 1.
- `/ext/out/external` is a subdirectory containing downloaded files necessary for
  certain parts of some notebooks. These downloads will happen automatically.

## Python

This project uses [uv](https://docs.astral.sh/uv/) to manage the Python environment.

Try to put as much code as you can into `/src/bdproject` since it can be shared between notebooks
and is git friendly. Make one notebook per task instead of putting everything in a single notebook.

Also please use the recommended extensions and settings (including the autoformatter on save) so we don't
have any "works on my machine" problems and don't get git diffs caused by formatting changes.
Notebook autoformat is semi-broken: You have to run `Ctrl+Shift+P` + `Ruff: Format imports` manually.

## Notebooks

**Make sure you use `./kernel.sh` instead of running the notebook on the login node.**
Run it, wait for it to spit out a link (might take a while because arnes).
Use the final link that's not 127.0.0.1, `jupyter server` likes to print garbage.
Inside VSCode:

- Open a notebook
- Click the top right corner "Select kernel"
- "Select Another Kernel"
- "Existing Jupyter Server"
- Copy-paste the link in there, press enter
- Wait 10 minutes because arnes
- You have to do this again every 24 hours, and every single time you restart the script :)

One notebook per task.
Put every single import and the autoreload magic in the first code cell.
This cell should be under the "Initialization" section, which should be idempotent and can be executed
without scrolling using VSCode's outline menu.
Useful if Python kills itself and you need to rerun the notebook.

In general, you should try to organize your code like this - sections you can reliably run using VSCode's outline panel
without having to first run every single previous cell, but just the obvious dependencies.
For example, a notebook like this:

- Initialization (imports and stuff)
- Ingest messy data and do a reshuffle that takes 2 hours
- Do some analysis on that data
  - Initialization (load the dataframes produced by the cleanup)
  - Plot 1
  - Plot 2
  - ...

That way, you can have really slow cells and fast cells in the same notebook without having to scroll every time
you need to restart the notebook because a high quality library incorrectly handles a `KeyboardInterrupt`.

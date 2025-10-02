import os
import time
from contextlib import contextmanager
from importlib import reload
from pathlib import Path

import cloudpickle
import itables
import seaborn as sns
from IPython import get_ipython
from IPython.core.magic import Magics, cell_magic, magics_class
from IPython.display import display
from matplotlib.figure import Figure


def project_path() -> Path:
    return Path(__file__).parent.parent.parent


def disp[T](value: T) -> T:
    display(value)
    return value


@contextmanager
def _with_source_date_epoch():
    original = os.environ.get("SOURCE_DATE_EPOCH")
    os.environ["SOURCE_DATE_EPOCH"] = "0"
    try:
        yield
    finally:
        if original is None:
            del os.environ["SOURCE_DATE_EPOCH"]
        else:
            os.environ["SOURCE_DATE_EPOCH"] = original


def savefig(fig: Figure, path: Path | str):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with _with_source_date_epoch():
        fig.savefig(p, bbox_inches=None)


@magics_class
class TimeAndSave(Magics):
    @cell_magic
    def time_and_save(self, line: str, cell):
        output_variable_name = line.strip()
        assert output_variable_name.isidentifier()

        t_start = time.perf_counter_ns()
        try:
            self.shell.run_cell(cell)
        finally:
            t_end = time.perf_counter_ns()
            self.shell.user_ns[output_variable_name] = t_end - t_start


def init():
    import bdproject

    reload(bdproject)
    os.chdir(project_path())
    get_ipython().register_magics(TimeAndSave)
    cloudpickle.register_pickle_by_value(bdproject)
    itables.init_notebook_mode()
    sns.set_style("whitegrid")

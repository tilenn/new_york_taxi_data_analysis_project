from warnings import filterwarnings

import cloudpickle

import bdproject

filterwarnings(
    "ignore",
    r"^The `Worker\..+` attribute has been moved to `Worker\.state\..+`$",
    FutureWarning,
)
cloudpickle.register_pickle_by_value(bdproject)

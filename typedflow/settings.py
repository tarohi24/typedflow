from pathlib import Path


project_root: Path = Path(__file__).parent.parent
logdir: Path = project_root.joinpath('log')

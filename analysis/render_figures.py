#!/usr/bin/env python3
"""Rebuild the Section VI figures from the bundled or user-generated tables."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory

from matplotlib.font_manager import FontProperties, findfont

from glas_analysis.four_panel import FourPanelBuilder

HERE = Path(__file__).resolve().parent
REFERENCE = HERE / "reference"
PROVENANCE = REFERENCE / "provenance.json"


def copy_sources(sources, project, experiments, provenance):
    outputs = project / "outputs"
    for phase in experiments:
        expected = [p for p in provenance["tables"] if p.startswith(phase + "/")]
        for relative in expected:
            source = sources / relative
            if not source.exists() and source.suffix == ".json" and sources != REFERENCE:
                # Some reference provenance records concern historical data only.
                if relative not in {"e5/e5_a_shard_gas_distribution/validation.json",
                                    "e6/e6_shard_completion_time/source_validation.json"}:
                    continue
            data = source.read_bytes()
            if sources == REFERENCE:
                actual = hashlib.sha256(data).hexdigest()
                if actual != provenance["tables"][relative]["distributed_sha256"]:
                    raise ValueError(f"Reference source checksum mismatch: {relative}")
            target = outputs / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)


def render(experiments, sources, output, font_family):
    provenance = json.loads(PROVENANCE.read_text(encoding="utf-8"))
    with TemporaryDirectory(prefix="glaschain-figures-") as tmp:
        project = Path(tmp)
        style = project / "styles/paper.mplstyle"
        style.parent.mkdir()
        base_style = (HERE / "styles/paper.mplstyle").read_text(encoding="utf-8")
        style.write_text(base_style.replace("Times New Roman", font_family), encoding="utf-8")
        copy_sources(sources, project, experiments, provenance)

        env = {**os.environ, "MPLBACKEND": "Agg", "PYTHONDONTWRITEBYTECODE": "1"}
        builder = FourPanelBuilder(project, output, font_family)
        results = {}
        for phase in experiments:
            record = provenance["figures"][phase]
            panels = []
            for item in record["panels"]:
                stem = Path(item["source_notebook"]).stem
                script = HERE / "panels" / f"{stem}.py"
                subprocess.run([sys.executable, str(script)], cwd=project, env=env, check=True)
                panels.append((f"{phase}/{stem}/{stem}.pdf", item["legend_caption"]))
            filename = record["manuscript_filename"]
            result = builder.compose_row(Path(filename).stem, panels)
            (output / "panels").mkdir(exist_ok=True)
            for filename_rel, _ in panels:
                folder = Path(filename_rel).parent
                shutil.copytree(project / "outputs" / folder, output / "panels" / folder,
                                dirs_exist_ok=True)
            results[phase] = result
            print(output / filename, flush=True)
        record = {"font_family": font_family, "reference_tables": sources == REFERENCE, "figures": results}
        (output / "figure_sources.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    phases = [f"e{i}" for i in range(1, 7)]
    parser.add_argument("--experiments", nargs="+", choices=phases, default=phases)
    parser.add_argument("--sources", type=Path, default=REFERENCE,
                        help="Directory containing phase-relative figure-source tables")
    parser.add_argument("--out", type=Path, default=HERE.parent / "results" / "figures")
    parser.add_argument("--font", help="Matplotlib font family (default Times New Roman when installed, else DejaVu Serif)")
    args = parser.parse_args()
    family = args.font or "Times New Roman"
    try:
        findfont(FontProperties(family=family), fallback_to_default=False)
    except ValueError:
        if args.font:
            raise
        family = "DejaVu Serif"
    render(args.experiments, args.sources.resolve(), args.out.resolve(), family)


if __name__ == "__main__":
    main()

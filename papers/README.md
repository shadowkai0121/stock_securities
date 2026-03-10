# Papers Workspace

Paper artifacts are generated under:

`papers/<paper_id>/`

Each paper workspace follows:

- `manuscript/`
- `tables/`
- `figures/`
- `appendix/`
- `reproducibility/`

A static scaffold is provided under `papers/template/` as a reference structure.

The manuscript folder is scaffolded with:

- `abstract.md`
- `introduction.md`
- `literature_review.md`
- `empirical_design.md`
- `results.md`
- `conclusion.md`

Generate artifacts directly from a run:

```bash
python -m research.paper_outputs.generate --experiment <run_id> --paper <paper_id>
```

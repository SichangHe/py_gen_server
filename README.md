# PyGenServer

TODO: summary

## Usage

TODO

## Type checking

Add the following to `pyproject.toml` (enabled here) for Pyright to validate subclasses of `Actor`:

```toml
[tool.pyright]
typeCheckingMode = "basic"
reportIncompatibleMethodOverride = "error"
```



test: 
  uv run pytest

build:
  uv build

publish:
  export UV_PUBLISH_TOKEN=$(grep -A 5 "\[pypi\]" ~/.pypirc | grep "password" | cut -d = -f 2 | tr -d ' ')
  uv publish

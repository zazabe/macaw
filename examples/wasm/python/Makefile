PYTHON ?= python3
VENV := .venv
COMPONENTIZE := $(VENV)/bin/componentize-py
WIT := ../../../macaw-wasm/wit
WORLD := macaw:http/plugin@1.0.0
OUTPUT := target/http-auth-plugin-python.wasm

.PHONY: build bindings clean

build: $(OUTPUT)

$(COMPONENTIZE): requirements-build.txt
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -r requirements-build.txt
	touch $(COMPONENTIZE)

bindings: $(COMPONENTIZE)
	$(COMPONENTIZE) -d $(WIT) -w $(WORLD) bindings .

$(OUTPUT): app.py $(WIT)/http-v1.wit $(COMPONENTIZE)
	mkdir -p target
	$(COMPONENTIZE) -d $(WIT) -w $(WORLD) componentize app -o $(OUTPUT)

clean:
	rm -rf $(VENV) target wit_world

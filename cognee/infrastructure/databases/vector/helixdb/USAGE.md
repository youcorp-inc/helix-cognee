# Usage

## Step one (template setup)

### clone the helix-cognee templates

```bash
git clone --branch feat/cognee-template https://github.com/HelixDB/helix-congee.git
```

### push/deploy the templates

```bash
cd helix-congee
```

### rename the helix.toml.bhs file to helix.toml

``` bash
mv helix.toml.bhs helix.toml
```

### edit the helix.toml file line 2 `name = "{{project_name}}"` to your project name e.g. `name = "cognee"`

### push the templates

``` bash
helix push {project-name}
```


## Step two (cognee setup)

### clone helix-congee(helix graph adapter for cognee)

```bash
git clone --branch feat/helixdb-graph-adapter https://github.com/HelixDB/helix-congee.git
```

### setup 

```bash
cd helix-congee
```

### create venv and activate

```bash
uv venv
```

```bash
source .venv/bin/activate
```

### install dependencies

```bash
uv sync --dev --all-extras
```

### copy the .env.template to .env

```bash
cp .env.template .env
```

### Add openAI api key for vector search (uncomment line 18)
```
#LLM_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### Run the helixdb vector integration test

```bash
uv run python cognee/tests/test_helixdb_vector.py
```

# MIMIC-IV intensive care unit visualization

# Data transformation using data build tool (`dbt`)

Run the following:

```
brew install uv
uv pip compile requirements.in -o requirements.txt
python3 -m venv .venv
source .venv/bin/activate # or for fish: source .venv/bin/activate.fish
pip install -r requirements.txt
```

Then initialize the dbt project (only needs to be run once per repository):

```
dbt init data_processing
```

Edit the `profiles.yml` file appropriately in `/Users/me/.dbt/profiles.yml`:

```yaml
data_processing:
  target: dev
  outputs:
    dev:
      type: duckdb
      # path: 'file_path/database_name.duckdb'
      extensions:
        - httpfs
        - parquet
      settings:
        s3_region: my-aws-region
        # s3_access_key_id: "{{ env_var('S3_ACCESS_KEY_ID') }}"
        # s3_secret_access_key: "{{ env_var('S3_SECRET_ACCESS_KEY') }}"
```

## Creating a dbt model for MIMIC-IV data
Then create a new model to transform the data, using the headers of the raw csv files downloaded from the FTP server:

```
❯ ls ~/data/physionet.org/files/mimiciv/3.0/hosp/admissions.csv.gz 
…3.0/hosp/admissions.csv.gz        …3.0/hosp/d_labitems.csv.gz   …3.0/hosp/microbiologyevents.csv.gz  …3.0/hosp/prescriptions.csv.gz 
…3.0/hosp/diagnoses_icd.csv.gz     …3.0/hosp/emar.csv.gz         …3.0/hosp/omr.csv.gz                 …3.0/hosp/procedures_icd.csv.gz
…3.0/hosp/drgcodes.csv.gz          …3.0/hosp/emar_detail.csv.gz  …3.0/hosp/patients.csv.gz            …3.0/hosp/provider.csv.gz      
…3.0/hosp/d_hcpcs.csv.gz           …3.0/hosp/hcpcsevents.csv.gz  …3.0/hosp/pharmacy.csv.gz            …3.0/hosp/services.csv.gz      
…3.0/hosp/d_icd_diagnoses.csv.gz   …3.0/hosp/index.html          …3.0/hosp/poe.csv.gz                 …3.0/hosp/transfers.csv.gz     
…3.0/hosp/d_icd_procedures.csv.gz  …3.0/hosp/labevents.csv.gz    …3.0/hosp/poe_detail.csv.gz 
```

```
❯ ls ~/data/physionet.org/files/mimiciv/3.0/icu/caregiver.csv.gz 
…/3.0/icu/caregiver.csv.gz       …/3.0/icu/d_items.csv.gz   …/3.0/icu/ingredientevents.csv.gz  …/3.0/icu/procedureevents.csv.gz
…/3.0/icu/chartevents.csv.gz     …/3.0/icu/icustays.csv.gz  …/3.0/icu/inputevents.csv.gz       
…/3.0/icu/datetimeevents.csv.gz  …/3.0/icu/index.html       …/3.0/icu/outputevents.csv.gz 
```

To print the headers:

```
for file in ~/data/physionet.org/files/mimiciv/3.0/{hosp,icu}/*.csv.gz; do
  if [ -f "$file" ]; then
    echo "File: $file"
    gzip -dc "$file" 2>/dev/null | head -n 6 | column -t -s,
    if [ $? -ne 0 ]; then
      echo "Error reading file. Check permissions or file integrity."
    fi
    echo "------------------------------"
  fi
done
```

To print a single header:

```
bash-5.2$ gzip -dc ~/data/physionet.org/files/mimiciv/3.0/hosp/admissions.csv.gz | head -n 2
```

Then ChatGPT or Claude can help build the data model for each file.

To execute a single dbt model, use the `--select` flag:

```
dbt run --select models/example/hosp/admissions.sql
```

If this step completes successfully, you can test it by running a duckdb SQL query against the parquet database file you just created:

A command such as `duckdb -c "SELECT * FROM '~/data/physionet.org/processed/mimiciv/hosp/admissions.parquet' LIMIT 10;"` should return the first 10 rows of the transformed data.

Here is what the output looks like:

```
❯ duckdb -c "SELECT * FROM '~/data/physionet.org/processed/mimiciv/hosp/admissions.parquet' LIMIT 10;"
┌────────────┬──────────┬─────────────────────┬─────────────────────┬───┬─────────────────────┬─────────────────────┬──────────────────────┐
│ subject_id │ hadm_id  │      admittime      │      dischtime      │ … │      edregtime      │      edouttime      │ hospital_expire_flag │
│   int32    │  int32   │      timestamp      │      timestamp      │   │      timestamp      │      timestamp      │       boolean        │
├────────────┼──────────┼─────────────────────┼─────────────────────┼───┼─────────────────────┼─────────────────────┼──────────────────────┤
```

## Old README

# mimic-iv-dbt-duckdb-visualization
Using dbt and duckdb to transform and visualize MIMIC IV intensive care unit data from an emergency department.

## Downloading the data

Download the data from PhyioNet: https://physionet.org/content/mimiciv/3.0/

Replace `USERNAME` with your credentialed username after completing the training and signing the data use agreement:

```
wget -r -N -c -np --user USERNAME --ask-password https://physionet.org/files/mimiciv/3.0/
```

## Ensuring compliance with HIPAA

We follow the National Institute of Health guidelines for clinical data repositories such as All of Us, and do not include any output for public display with fewer than 20 individuals.

# Observable Framework

```
npx @observablehq/framework@latest create
```

## Notes for website deployment

This is an [Observable Framework](https://observablehq.com/framework) app. To start the local preview server, run:

```
yarn dev
```

Then visit <http://localhost:3000> to preview your app.

For more, see <https://observablehq.com/framework/getting-started>.

## Project structure

A typical Framework project looks like this:

```ini
.
├─ src
│  ├─ components
│  │  └─ timeline.js           # an importable module
│  ├─ data
│  │  ├─ launches.csv.js       # a data loader
│  │  └─ events.json           # a static data file
│  ├─ example-dashboard.md     # a page
│  ├─ example-report.md        # another page
│  └─ index.md                 # the home page
├─ .gitignore
├─ observablehq.config.js      # the app config file
├─ package.json
└─ README.md
```

**`src`** - This is the “source root” — where your source files live. Pages go here. Each page is a Markdown file. Observable Framework uses [file-based routing](https://observablehq.com/framework/routing), which means that the name of the file controls where the page is served. You can create as many pages as you like. Use folders to organize your pages.

**`src/index.md`** - This is the home page for your app. You can have as many additional pages as you’d like, but you should always have a home page, too.

**`src/data`** - You can put [data loaders](https://observablehq.com/framework/loaders) or static data files anywhere in your source root, but we recommend putting them here.

**`src/components`** - You can put shared [JavaScript modules](https://observablehq.com/framework/javascript/imports) anywhere in your source root, but we recommend putting them here. This helps you pull code out of Markdown files and into JavaScript modules, making it easier to reuse code across pages, write tests and run linters, and even share code with vanilla web applications.

**`observablehq.config.js`** - This is the [app configuration](https://observablehq.com/framework/config) file, such as the pages and sections in the sidebar navigation, and the app’s title.

## Command reference

| Command           | Description                                              |
| ----------------- | -------------------------------------------------------- |
| `yarn install`            | Install or reinstall dependencies                        |
| `yarn dev`        | Start local preview server                               |
| `yarn build`      | Build your static site, generating `./dist`              |
| `yarn deploy`     | Deploy your app to Observable                            |
| `yarn clean`      | Clear the local data loader cache                        |
| `yarn observable` | Run commands like `observable help`                      |

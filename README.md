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

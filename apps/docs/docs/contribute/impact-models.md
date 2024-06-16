---
title: Write a Data Model
sidebar_position: 5
---

:::info
[dbt (data build tool)](https://www.getdbt.com/blog/what-exactly-is-dbt) is a
command line tool that enables data analysts and engineers to transform data in
the OSO data warehouse more effectively. dbt transforms are written in SQL. We
use them most often to define impact metrics and materialize aggregate data
about projects. You can contribute dbt transforms to create new impact metrics
in our data warehouse.
:::

[oso]: https://github.com/opensource-observer/oso
[oss-directory]: https://github.com/opensource-observer/oss-directory

At some point, you will probably want to start doing more deep data science,
contributing models, or even contributing data. Normally, there are quite a few
steps to complete this task - setting up GCP service accounts, connecting the BigQuery
datasets, etc. We decided to automate much of that with our Wizard. Let's get started!

## Getting Started with dbt

OSO uses dbt to analyze the data in our public data warehouse on BigQuery. This
is all maintained within the [OSO monorepo][oso] and is open for contribution
from the community. This guide will walk you through adding a dbt model to the repository.
It assumes a basic understanding of SQL and the command line.

### System Prequisites

Before you begin you'll need the following on your system:

- Python >=3.11 (see [here](https://www.python.org/downloads/) if you
  don't have it installed)
- Python Poetry >= 1.8 (see [here](https://pypi.org/project/poetry/) to install it)
- git (see [here](https://github.com/git-guides/install-git) if you don't have it installed)
- A GitHub account (see [here](https://github.com/join) to open a new account)
- BigQuery access (see [here](../get-started/#login-to-bigquery) if you don't have it setup already)

### Install `gcloud`

If you don't have `gcloud`, we need this to manage GCP from the command line.
The instructions are [here](https://cloud.google.com/sdk/docs/install).

_For macOS users_: Instructions can be a bit clunky if you're on macOS, so we
suggest using homebrew like this:

```bash
brew install --cask google-cloud-sdk
```

### Fork and/or clone the OSO repo

Once you've got everything you need to begin, you'll need to get the [OSO
repository](https://github.com/opensource-observer/oso) and clone it to your
local system (replace this url if you've forked the repository):

```bash
git clone https://github.com/opensource-observer/oso.git
```

After that process, has completed. `cd` into the oso repository:

```bash
cd oso
```

### Run the Wizard

First, authenticate with `gcloud`:

```bash
gcloud auth application-default login
```

Next, install the Python dependencies and run the wizard. It will ask you
to run `dbt` at the end (Say yes if you'd like to copy the
oso_playground dataset). Simply run:

```bash
poetry install && poetry run oso_lets_go
```

:::tip
Under the hood, `oso_lets_go` will create a GCP project
and BigQuery dataset if they don't already exist,
and copy a small subset of the OSO data for you to develop against,
called `playground`.
It will also create a dbt profile to connect to this dataset
(stored in `~/.dbt/profiles.yml`).
The script is idempotent, so you can safely run it again
if you encounter any issues.
:::

Once this is completed, you'll have a full "playground" of your own.

We strongly recommend running everything in the `poetry shell` so you don't have to
type `poetry run` all the time. You can do this by running:

```bash
poetry shell
```

Finally, you can test that everything is working by running the following command:

```bash
dbt run
```

This will run the full dbt pipeline against your own
copy of the OSO playground dataset.

## Working with OSO dbt Models

### How OSO dbt models are organized

From the root of the oso repository, dbt models can be found at
`warehouse/dbt/models`.

OSO's repository organizes dbt models following the suggested directory
structure from DBT's own best practices guides ([see
here for a fuller explanation](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview))

- `staging` - This directory contains models used to clean up source data.
  Unless cost prohitibitive, all of these models should be materialized as
  views. Subdirectories within `staging` are organized by the source data.
- `intermediate` - This directory contains models that transform staging data
  into useful representations of the raw warehouse data. These representations
  are not generally intended to be materialized as tables but instead as views.
- `marts` - This directory contains transformations that should be fairly
  minimal and mostly be aggregations. In general, `marts` shouldn't depend on
  other marts unless they're just coarser grained aggregations of an upstream
  mart. Marts are also automatically copied to the frontend database that
  powers the OSO API and website.

### OSO data sources

The titles for these sections reflect the directories available in
the `staging` directory of our dbt models.

This isn't an exhaustive list of all data sources but instead a list of data
sources that OSO currently relies upon. If you wish to use other available
public datsets on bigquery. Please feel free to add a model that references that
data source!

#### The `oso_source` macro

For referencing sources, you should use the `oso_source()` macro which has the
same parameters as the built in `source()` macro from DBT. However, the
`oso_source()` macro includes logic that is used to help manage our public
playground dataset.

#### The `oss-directory` source

The OSO community maintains a directory of collections and projects called
[oss-directory][oss-directory].
Additionally, we use the list of project's repositories to gather additional
information on each repository from github. The source data is referenced as `{{
oso_source('ossd', '{TABLE_NAME}') }}` where `{TABLE_NAME}` could be one of the
following tables:

- `collections` - This data is pulled directly from the [oss-directory
  repository][oss-directory] and is
  groups of projects. You can view this table
  [here][collections_table]
- `projects` - This data is also pulled directly from the oss-directory
  repository. It describes a project's repositories, blockchain addresses, and
  public packages. You can view this table
  [here][projects_table]
- `repositories` - This data is derived by gathering repository data of all the
  unique repositories present within the `projects` table. You can view this
  table [here][repositories_table]

[collections_table]: https://console.cloud.google.com/bigquery?project=opensource-observer&ws=!1m5!1m4!4m3!1sopensource-observer!2soso!3scollections_ossd
[projects_table]: https://console.cloud.google.com/bigquery?project=opensource-observer&ws=!1m5!1m4!4m3!1sopensource-observer!2soso!3sprojects_ossd
[repositories_table]: https://console.cloud.google.com/bigquery?project=opensource-observer&ws=!1m5!1m4!4m3!1sopensource-observer!2soso!3srepositories_ossd

#### The `github_archive` source

Referenced as `{{ source('github_archive', 'events') }}`, this data source is an
external BigQuery dataset that is maintained by [GH Archive][gharchive]. It is
not suggest that you use this data source directly as doing so can be cost
prohibitive. We would, instead, suggest that you use `{{
ref('stg_github__events') }}` as this is the raw github archive data only for
the projects within the [oss-directory][oss-directory].

For more information we on the GH Archive and what you might find in the raw
data, we suggest you read more at [GH Archive][gharchive]

[gharchive]: https://www.gharchive.org

#### The `dune` source

In order to have collected blockchain data, the OSO team has used dune in the
past (we may not continue to use so into the future) to collect blockchain
transaction and trace data related to the projects in oss-directory. Currently,
the only data available in this dataset is `arbitrum` related transactions and
traces. That collected data is available as a data source that can be referenced
as `{{ oso_source('dune', 'arbitrum') }}`. We also have Optimism data, but that is
currently an export from our legacy data collection. We will expose that as well,
so check back soon for more updates!

### A note about `_id`'s

Due to the diversity of data sources and event types, the ID system used by the
data warehouse might not be immediately obvious to anyone who's starting their
journey with the OSO dbt models.

As a general rule for our dataset, anything that is in the `marts` directory
should that has an ID should generate the ID using the `oso_id()` macro. This
macro generates a url safe base64 encoded identifier from a hash of the
namespace of the identifier and the ID within that namespace. This is done to
simplify some table joins at later stages (so you don't need to match on
multiple dimensions). An example of using the macro within the `collections`
namespace for a collection named `foo` would be as follows:

```jinja
{{ oso_id('collection', 'foo')}}
```

## Adding Your dbt Model

Now you're armed with enough information to add your model! Add your model to
the directory you deem fitting. Don't be afraid of getting it wrong, that's all
part of our review process to guide you to the right place.

Your model can be written in SQL. We've included some examples to help you get started.

### Running your model

Once you've updated any models you can run dbt _within the poetry environment_ by simply calling:

```bash
dbt run
```

_Note: If you configured the dbt profile as shown in this document, this `dbt
run` will write to the `opensource-observer.oso_playground` dataset._

It is likely best to target a specific model when developing
so things don't take so long on some of our materializations:

```bash
dbt run --select {name_of_your_model}
```

If `dbt run` runs without issue and you feel that you've completed something you'd
like to contribute. It's time to open a PR!

### Using the BigQuery UI to check your queries

During your development process, it may be useful to use the BigQuery UI to
execute queries.

In the future we will have a way to connect your own
infrastructure so you can generate models from our staging repository. However,
for now, it is best to compile the dbt models into their resulting BigQuery SQL
and execute that on the BigQuery UI. To do this, you'll need to run `dbt
compile` from the root of the [oso Repository][oso] like so:

```bash
dbt compile
```

_You'll want to make sure you're also in the `poetry shell` otherwise you won't
use the right dbt binary_

Once you've done so you will be able to find your model compiled in the
`target/` directory (this is in the root of the repository). Your model's
compiled sql can be found in the same relative path as it's location in
`warehouse/dbt/models` inside the `target/` directory.

The presence of the compiled model does not necessarily mean your SQL will work
simply that it was rendered by `dbt` correctly. To test your model it's likely
cheapest to copy the query into the [BigQuery
Console](https://console.cloud.google.com/bigquery) and run that query there.

### Submit a PR

Once you've developed your model and you feel comfortable that it will properly
run, you can submit it a PR to the [oso repository][oso] to be tested by the OSO
GitHub CI workflows.

### DBT model execution schedule

After your PR has been approved and merged, it will automatically be deployed
into our BigQuery dataset and available for querying. At this time, the data
pipelines are executed once a day by the OSO CI at 02:00 UTC. The pipeline
currently takes a number of hours and any materializations or views would likely
be ready for use by 4-6 hours after that time.

You can monitor all pipeline runs in
[GitHub actions](https://github.com/opensource-observer/oso/actions/workflows/warehouse-run-data-pipeline.yml).

## Model References

All OSO models can be found in
[`warehouse/dbt/models`](https://github.com/opensource-observer/oso/tree/main/warehouse/dbt/models).

We also continuously deploy model reference documentation at
[https://models.opensource.observer/](https://models.opensource.observer/)

---
title: Insights Repo
sidebar_position: 1
---

Our [insights repo](https://github.com/opensource-observer/insights) is the place to share analysis and visualizations. It contains insights and exploratory data analysis on the health of open source software ecosystems.

Juypter Notebooks are included so others can get inspiration and understand/improve upon our analysis. Where practical, copies of data used to generate insights has been saved in CSV or JSON format. However, many of the notebooks rely on direct queries to the data warehouse and therefore will not run locally without API access. These notebooks are not actively maintained and links will go stale over time.

## Repository Structure

Here's an overview of the repository structure:

```
analysis
└── dependencies
└── ecosystem_reports
└── XYZ chain ...

community
└──datasets
└──notebook_templates

experiments
└── bounties
└── etc...

scripts
visualizations
```

## Analysis

The [analysis folder](https://github.com/opensource-observer/insights/tree/main/analysis) contains the bulk of the analysis and insights, mostly in the form of Jupyter Notebooks that leverage the OSO data warehouse and API. These are mainly provided for reference and to provide inspiration for others. They are not actively maintained and links will go stale over time. The dependencies folder contains scripts that are used to generate the dependency graph visualizations. The ecosystem_reports folder contains reports that are generated on a regular basis and published to the OSO blog.

## Community

The community folder contains datasets and [notebook templates](./notebooks) that may be useful to new contributors. These provide sample queries and visualization tools, as well as steps for reproducing analysis that goes into the ecosystem reports.

The community folder also contains our [data bounties](./bounties).

## Scripts

The scripts folder contains scripts for fetching data from the OSO API and data warehouse. It also includes some scripts for fetching GitHub or blockchain data on an ad hoc basis.

## Visualizations

The visualizations folder contains visualizations that are used in many of the reports, such as sankeys and heatmaps. Most of our visualizations are generated in Python using matplotlib, seaborn, and plotly.

## API Access

In order to use OSO the data warehouse, you will need to request an API key. Please apply to join to the Data Collective [here](https://www.opensource.observer/data-collective).

# Crime data engineering

#### (Combined notebook code [here](8-combined-AUTO-GENERATED-FILE.ipynb) - auto-generated file with all notebooks combined)

The goal is to create a single pair of datasets (one by county, one by district) that can be used by a web front-end (docs & source [here](https://github.com/ryayoung/coloradoplot), live app [here](https://colorado-crime.herokuapp.com/)) to visualize a wide variety of geocoded public colorado data on a map. The resulting data has ~350 geocoded metrics for each county for 8 consecutive years, and ~140 geocoded metrics for each school district.

### The problem
- Public data for Colorado on [data.colorado.gov](https://data.colorado.gov/) comes from a variety of sources.
- It is **NOT** clean
- It does **NOT** have consistently formatted keys upon which we can join
- Most of it is **NOT** yet geocoded
- Each dataset has some sort of version of a 'county' or 'district' column, but they are formatted differently

#### This project will:
- Create a consistent naming format for counties and districts and a way to repeatedly convert new columns to this format
- Get geographic boundary vectors and center coordinates for each district and each county
- Clean and engineer source data for analysis/visualization
- Join all data for counties and districts respectively by year, including geographic vectors

To run this project yourself, simply follow the notebooks in numeric order, 1-7
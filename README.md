# Abidjan Housing Dataset

Kinda equivalent to the _California Housing dataset_ (you can find it on [scikit-learn](https://inria.github.io/scikit-learn-mooc/python_scripts/datasets_california_housing.html)). 
This dataset is resulting from data scraped from the popular listing website [Jumia Deals](https://deals.jumia.ci/).
The raw data is already available (see [releases](https://github.com/marcaureln/abidjan-housing-dataset/releases)).

## Getting started

**Prerequisite**: Python 3 and [Poetry](https://python-poetry.org/docs/).

```bash
# Clone the repository:
git clone https://github.com/marcaureln/abidjan-housing-dataset.git
# Install dependencies:
poetry install
# Activate the virtual environment:
poetry shell
```

To leave the virtual environment, run `exit`.

### Run spiders

```bash
# Move to Scrapy project folder:
cd ./scraper
# Copy the example configuration file:
cp scraper.toml.example scraper.toml
# Edit the configuration file with your favorite editor (e.g. vim):
vim scraper.toml
# Gather posts links
scrapy crawl links
# Gather posts details 
scrapy crawl posts
```

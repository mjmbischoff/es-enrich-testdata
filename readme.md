# Synthetic data generator
Uses 
- open pagerank data set see ['what is openpagerank'](https://www.domcop.com/openpagerank/what-is-openpagerank) for more information
- opencsv for parsing the csv
- jackson for json generation
- javafaker for fancier fake data
- apache commons compression, since [Rally](https://github.com/elastic/rally) convention is bzip: we bzip our data.

## The following files are generated:
| name                       | description                                                   |
|----------------------------|---------------------------------------------------------------|
|stats                       | contains stats and other information around what we generated |
|top10milliondomains.csv     | extracted csv from openpagerank                               |
|top10milliondomains.json    | openpagerank as new line json                                 |
|top10milliondomains.json.bz | openpagerank as new line json, bzipped                        |
|user_activity.json          | fake dns activity data                                        |
|user_activity.json.bz       | fake dns activity data, bzipped                               |
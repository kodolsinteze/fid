Task - Join two sets of data

Datasets:

https://data.gov.lv/dati/dataset/4de9697f-850b-45ec-8bba-61fa09ce932f/resource/25e80bf3-f107-4ab4-89ef-251b5b9374e9/download/register.csv
Municipal Open Data
https://www.epakalpojumi.lv/odata/service/Districts
Connected the two data sets and as a result obtained a set that contains Business Register data on company registration numbers (regcode), company names (name), company addresses (address) and the open data of Municipalities on Municipal ATVK codes (district_atvk) and names of municipalities (district_name).

The data sets do not contain a common key, but the UR data contain business addresses that usually include an abbreviated county name, and the municipal data contain exact county names. The task is to retrieve the county name from the address field and try to find its matching full county name to connect the datasets. Within the scope of the task, it is not necessary to find completely all matches, but as many as possible, avoiding erroneous matches as much as possible.

The resulting data set mustis written in a sqlite database table.

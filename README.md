# houspiders

## Setup
1. Create a virtual environment
2. `pip install -r requirements.txt`

## Crawlers

The cralwer has folliwing 3 component:

### 1. house_list_spider 
It will crawl the house list every day and dump to folder `output/YYYYMMDD/house_links.csv`

### 2. house_list_processor
It will scan the new house_list info after house_list_spider finished 
and output a feed to be consumed by house_info_spider:

#### Strategy 1 (less fresh house page, less http requests): 

 1. Add new listed houses to `house_link` table and put them into feed 
 2. For those houses with updated price or was unavailable, 
    we merge its info to `house_link` and put it into feed;
 3. For remaining house, they are existing house w/o updated price, we do nothing 
 4. For those available house not showing up in latest house_list, put them into feed;

#### Strategy 2 (more fresh house page, more http requests): 

 Different from strategy 1 on 3. -- still write these house linkds to feed.
 Basically this strategy always request all available houses.

### 3. house_info_processor
It will scan the feed generated by new_house_list_processor and try to crawl the 
raw_html for each house item based on follow strategy:

 1. If the page is not available mark it as unavailable in `house_link` table and update the unavailable_date;
 2. If the page is available, mark it as available in `house_link` table 
    and update its price in  `house_price_history` table if necessary;
    Then save the page and run `house_info_extractor` to inject/merge new data into `house_info` table;
        
## Data Analyser

### 1. daily_stats_runner
We want to write the daily stats to a `house_daily_stats` table to extract
most valuable pending houses per geo.


from loaders.landing_to_bronze_daily import main as landing_to_bronze_daily
from loaders.bronze_index_create import main as bronze_index_create
from loaders.loading_to_bronze_main import main as loading_to_bronze_main
from loaders.landing_to_silver_all import main as landing_to_silver
from loaders.landing_to_gold import main as landing_to_gold
from loaders.archive_source_file import main as archive_source_file
from loaders.performance_boost import main as performance_boost
import time as time

if __name__ == "__main__":
    start = time.time()
    loading_to_bronze_main()
    landing_to_bronze_daily()
    bronze_index_create()
    landing_to_silver()
    landing_to_gold()
    archive_source_file()   
    performance_boost()                                            
    print(f"Total execution time: {time.time() - start} seconds")



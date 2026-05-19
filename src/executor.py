
from loaders.landing_to_bronze_daily import main as loading_to_bronze_daily
from loaders.bronze_index_create import main as bronze_index_create
from loaders.loading_to_bronze_main import main as loading_to_bronze_main
import time as time

if __name__ == "__main__":
    start = time.time()
    loading_to_bronze_main()
    loading_to_bronze_daily()
    bronze_index_create()
    print(f"Total execution time: {time.time() - start} seconds")







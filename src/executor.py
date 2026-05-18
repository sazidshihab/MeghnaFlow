
from loaders.landing_to_bronze_daily import main as loading_to_bronze_daily
from loaders.bronze_daily_to_bronze_main import main as bronze_daily_to_bronze_main
import time as time

if __name__ == "__main__":
    start = time.time()
    loading_to_bronze_daily()
    bronze_daily_to_bronze_main()
    print(f"Total execution time: {time.time() - start} seconds")







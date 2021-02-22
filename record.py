from utils import initial, config, logger
import time

def main():
    users, market_client, target_time = initial()
    base_price, base_price_time = market_client.get_base_price(target_time)

    while True:
        now = time.time()
        if now > target_time + 300:
            break

        



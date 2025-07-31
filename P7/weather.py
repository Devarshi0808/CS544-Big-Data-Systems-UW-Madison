import datetime, random, time, os
from datetime import datetime, timedelta

def get_next_weather_main():
    start_date = datetime.strptime("2000-01-01", "%Y-%m-%d")

    i = 0
    while True:
        for station in "ABCDEFGHIJ":
            # I and J are some special stations for testing, others are random
            if station == "I":
                temp = 5
            elif station == "J":
                temp = i % 10
            else:
                temp = random.gauss(30, 15)
            yield start_date.strftime("%Y-%m-%d"), float(temp), f"Station{station}"
        start_date += timedelta(days=1)
        i += 1

def get_next_weather(delay_sec=1):
    if 'AUTOGRADER_DELAY_OVERRIDE_VAL' in os.environ:
        delay_sec = float(os.environ['AUTOGRADER_DELAY_OVERRIDE_VAL'])
    weather_generator = get_next_weather_main()
    while True:
        yield next(weather_generator)
        time.sleep(delay_sec)

if __name__ == "__main__":
    for record in get_next_weather(0.1):
        print(record)

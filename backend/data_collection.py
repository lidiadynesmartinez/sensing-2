import requests
import datetime
from pytrends.request import TrendReq
from backend.data_api import search, geo


WEATHER_KEY = "8b734637085f4b99bc2153812182411"
WEATHER_URL = "https://api.worldweatheronline.com/premium/v1/past-weather.ashx"


def collect_avg_week_weather_data(date, geo=''):
    start_date_string = date.strftime("%Y-%m-%d")
    end_date_string = (date + datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    print(geo)

    params = {
        "q": geo,
        "date": start_date_string,
        "enddate": end_date_string,
        "includelocation": "yes",
        "format": "json",
        "tp": 24,
        "key": WEATHER_KEY,
    }
    res = requests.get(WEATHER_URL, params=params).json()['data']
    try:
        weather = res['weather']
    except:
        return {}

    results = {'temp': [], 'desc': [], 'precip': [], 'sun': []}
    for day in weather:
        results['temp'].append(day['maxtempC'])
        results['desc'].append(day['hourly'][0]['weatherDesc'][0]['value'])
        results['precip'].append(day['hourly'][0]['precipMM'])
        results['sun'].append(day['sunHour'])
    avg_temp = int(sum([int(t) for t in results['temp']]) / len(results['temp']))
    mode_desc = max(set(results['desc']), key=results['desc'].count)
    avg_precip = sum([float(p) for p in results['precip']]) / len(results['precip'])
    avg_sun = sum([float(t) for t in results['sun']]) / len(results['sun'])

    return {
        'desc': mode_desc,
        'temp': avg_temp,
        'sun': avg_sun,
        'precip': avg_precip
    }


def get_week_starts(start_date, end_date):
    curr_day = start_date - datetime.timedelta(days=start_date.weekday())
    dates = []
    while True:
        dates.append(datetime.datetime.combine(curr_day, datetime.datetime.min.time()))
        curr_day = curr_day + datetime.timedelta(days=7)
        if curr_day > end_date:
            return dates


class DataCollection:
    def __init__(self, weather_key=WEATHER_KEY):
        self.weather_key = weather_key

    def collect_weekly_weather_data(self, start_date, end_date, geo_code=''):
        dates = get_week_starts(start_date, end_date)
        res = {}
        for date in dates:
            res[date] = collect_avg_week_weather_data(date, geo_code)
        return res

    def collect_weekly_trend_data(self, keyword, start_date, end_date, geo_code=''):
        week_starts = get_week_starts(start_date, end_date)
        start_date, end_date = week_starts[0], week_starts[-1]
        start_date_string = start_date.strftime("%Y-%m-%d")
        end_date_string = end_date.strftime("%Y-%m-%d")

        pytrends = TrendReq(hl='en-US', tz=360)
        pytrends.build_payload([keyword], cat=0, timeframe=f"{start_date_string} {end_date_string}", geo=geo_code, gprop='')
        return pytrends.interest_over_time().to_dict()[keyword]


if __name__ == "__main__":
    dc = DataCollection()
    for search_term, geo_code in search, geo:
        weather = dc.collect_weekly_weather_data(datetime.date.today() - datetime.timedelta(weeks=5), datetime.date.today() - datetime.timedelta(weeks=1), search_term)
        print(weather)

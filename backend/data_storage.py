from mongoengine import *
from mongoengine.context_managers import switch_db

from backend.data_collection import get_week_starts

DATABASE_NAME = 'webapp_db'

REMOTE_USER = "lidiadynes"
REMOTE_PW = "mLabpw19"
REMOTE_HOST = 'ds145194.mlab.com'
REMOTE_PORT = 45194


class Correlation(Document):
    keyword = StringField(required=True)
    geo_code = StringField(required=True)
    temp = FloatField(required=True)
    precip = FloatField(required=True)
    sun = FloatField(required=True)


class Trends(Document):
    keyword = StringField(required=True)
    geo_code = StringField(required=True)
    time = DateTimeField(required=True)
    interest = IntField(required=True)


class Weather(Document):
    geo_code = StringField(required=True)
    time = DateTimeField(required=True)
    temp = IntField(required=True)
    desc = StringField(required=True)
    precip = FloatField(required=True)
    sun = FloatField(required=True)


class User(Document):
    username = StringField(max_length=20, unique=True, required=True)
    email = EmailField(max_length=120, unique=True, required=True)
    password = StringField(max_length=60, required=True)
    location = StringField(max_length=60, required=True)
    image_file = ImageField(max_length=30, default='default.jpg', required=True)


class ResilientStorage:
    def __init__(self):
        self.local_alias = 'default'
        self.remote_alias = 'remote'
        register_connection(self.local_alias, DATABASE_NAME, host="localhost", port=27017)
        register_connection(self.remote_alias,
                            DATABASE_NAME,
                            host=REMOTE_HOST,
                            port=REMOTE_PORT,
                            username=REMOTE_USER,
                            password=REMOTE_PW)


class DataStorage(ResilientStorage):
    def insert_trend_data(self, collected_trends, geo_code, keyword):
        for time, interest in collected_trends.items():
            existing_local = Trends.objects(geo_code=geo_code, time=time, keyword=keyword)
            if existing_local is not None:
                existing_local.delete()
            Trends(geo_code=geo_code, time=time, interest=interest, keyword=keyword).save()

            with switch_db(Trends, self.remote_alias) as TrendsRemote:
                existing_remote = TrendsRemote.objects(geo_code=geo_code, time=time, keyword=keyword)
                if existing_remote is not None:
                    existing_remote.delete()
                TrendsRemote(geo_code=geo_code, time=time, interest=interest, keyword=keyword).save()

    def insert_weather_data(self, collected_weather, geo_code):
        for time, res in collected_weather.items():
            if 'temp' not in res:
                return

            existing_local = Weather.objects(geo_code=geo_code, time=time)
            if existing_local is not None:
                existing_local.delete()
            Weather(geo_code=geo_code,
                    time=time,
                    temp=res['temp'],
                    desc=res['desc'],
                    precip=res['precip'],
                    sun=res['sun']
                    ).save()

            with switch_db(Weather, self.remote_alias) as WeatherRemote:
                existing_remote = WeatherRemote.objects(geo_code=geo_code, time=time)
                if existing_remote is not None:
                    existing_remote.delete()
                WeatherRemote(geo_code=geo_code,
                              time=time,
                              temp=res['temp'],
                              desc=res['desc'],
                              precip=res['precip'],
                              sun=res['sun']
                              ).save()

    def get_weather_data(self, geo_code, start_date, end_date):
        local_res = Weather.objects(geo_code=geo_code, time__gte=start_date, time__lte=end_date)
        if len(local_res) > 0:
            return [w.to_mongo().to_dict() for w in local_res]

        with switch_db(Weather, self.remote_alias) as WeatherRemote:
            remote_res = WeatherRemote.objects(geo_code=geo_code, time__gte=start_date, time__lte=end_date)
            if len(remote_res) > 0:
                to_return = [w.to_mongo().to_dict() for w in remote_res]
                return to_return
        return []

    def get_trend_data(self, keyword, geo_code, start_date, end_date):
        local_res = Trends.objects(geo_code=geo_code, keyword=keyword, time__gte=start_date, time__lte=end_date)
        if len(local_res) > 0:
            return [r.to_mongo().to_dict() for r in local_res]

        with switch_db(Trends, self.remote_alias) as TrendsRemote:
            remote_res = TrendsRemote.objects(geo_code=geo_code, keyword=keyword, time__gte=start_date, time__lte=end_date)
            if len(remote_res) > 0:
                to_return = [r.to_mongo().to_dict() for r in remote_res]
                return to_return
        return []

    def get_missing_weather_ranges(self, start_date, end_date, geo_code):
        expected_dates = get_week_starts(start_date, end_date)
        missing_ranges = []
        missing_start = None
        for date in expected_dates:
            results = Weather.objects(time=date, geo_code=geo_code).count()
            if results == 0:
                with switch_db(Weather, self.remote_alias) as WeatherRemote:
                    results = WeatherRemote.objects(time=date, geo_code=geo_code).count()
            if results == 0 and missing_start is None:
                missing_start = date
            elif results != 0 and missing_start is not None:
                missing_ranges.append((missing_start, date))
                missing_start = None
        if missing_start is not None:
            missing_ranges.append((missing_start, expected_dates[-1]))
        return missing_ranges

    def get_missing_trend_ranges(self, start_date, end_date, geo_code, keyword):
        expected_dates = get_week_starts(start_date, end_date)
        missing_ranges = []
        missing_start = None
        for date in expected_dates:
            results = Trends.objects(time=date, geo_code=geo_code, keyword=keyword).count()
            if results == 0:
                with switch_db(Trends, self.remote_alias) as TrendsRemote:
                    results = TrendsRemote.objects(time=date, geo_code=geo_code).count()
            if results == 0 and missing_start is None:
                missing_start = date
            elif results != 0 and missing_start is not None:
                missing_ranges.append((missing_start, date))
                missing_start = None
        if missing_start is not None:
            missing_ranges.append((missing_start, expected_dates[-1]))
        return missing_ranges

    def clear_weather(self):
        Weather.objects.delete()

    def clear_trends(self):
        Trends.objects.delete()


class UserStorage(ResilientStorage):
    def insert_new_user(self, username, email, password, location):
        if self.validate_email(email) and self.validate_username(username):
            User(username=username,
                 email=email,
                 password=password,
                 location=location
                 ).save()
            return True
        return False

    def validate_username(self, username):
        if len(User.objects(username=username)) > 0:
            return False
        with switch_db(User, self.remote_alias) as UserRemote:
            return len(UserRemote.objects(username=username)) == 0

    def validate_email(self, email):
        if len(User.objects(email=email)) > 0:
            return False
        with switch_db(User, self.remote_alias) as UserRemote:
            return len(UserRemote.objects(email=email)) == 0

    def get_user_data(self, username):
        local = User.objects(username=username)
        if len(local > 0):
            return local[0].to_mongo().to_dict()
        with switch_db(User, self.remote_alias) as UserRemote:
            remote = UserRemote.objects(username=username)
            if len(remote > 0):
                return remote[0].to_mongo().to_dict()
        return {}

    def clear_user(self):
        User.objects.delete()
        with switch_db(User, self.remote_alias) as UserRemote:
            UserRemote.objects.delete()


class CorrelationStorage(ResilientStorage):
    def get_correlation(self, keyword, geo_code):
        res = Correlation.objects(keyword=keyword, geo_code=geo_code)
        if len(res) > 0:
            return {
                "precip": res[0].precip,
                "temp": res[0].temp,
                "sun": res[0].sun
            }

        with switch_db(Correlation, self.remote_alias) as CorrelationRemote:
            res = CorrelationRemote.objects(keyword=keyword, geo_code=geo_code)
            if len(res) > 0:
                return {
                    "precip": res[0].precip,
                    "temp": res[0].temp,
                    "sun": res[0].sun
                }

        return {}

    def insert_correlation(self, keyword, geo_code, precip, temp, sun):
        existing = Correlation.objects(keyword=keyword, geo_code=geo_code)
        if len(existing) > 0:
            existing.delete()
        Correlation(keyword, geo_code, temp, precip, sun).save()

        with switch_db(Correlation, self.remote_alias) as CorrelationRemote:
            existing = CorrelationRemote.objects(keyword=keyword, geo_code=geo_code)
            if len(existing) > 0:
                existing.delete()
            CorrelationRemote(keyword, geo_code, temp, precip, sun).save()

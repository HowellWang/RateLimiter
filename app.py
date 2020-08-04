from flask import Flask, request, Response
import redis
import time

CALL_PER_HOUR = 10
CALL_PER_DAY = 30

app = Flask(__name__)


@app.route('/')
def root():
    return Response('Ok')


@app.route('/rate_limiting')
def rate_limit():
    route_score = 1

    r = redis.Redis(host='localhost', port=6379, db=0)

    api = request.headers.get('X-API-Key')
    if not api:
        return Response('X-API-Key header is missing, please check!', status=401)

    epoch = int(time.time() * 1000)
    pipe = r.pipeline()

    pipe.zremrangebyscore('%s:hourly' % api, 0, epoch - 3600000)
    pipe.zadd('%s:hourly' % api, {'%d:%d' % (epoch, route_score): epoch})
    pipe.zrange('%s:hourly' % api, 0, -1)
    pipe.expire('%s:hourly' % api, 3600000)

    pipe.zremrangebyscore('%s:daily' % api, 0, epoch - 86400000)
    pipe.zadd('%s:daily' % api, {'%d:%d' % (epoch, route_score): epoch})
    pipe.zrange('%s:daily' % api, 0, -1)
    pipe.expire('%s:daily' % api, 864000000)

    res = pipe.execute()

    hour_score = sum(int(i.decode('utf-8').split(':')[-1]) for i in res[2])
    day_score = sum(int(i.decode('utf-8').split(':')[-1]) for i in res[6])

    if hour_score > CALL_PER_HOUR or day_score > CALL_PER_DAY:
        resp = Response('Data exceeded', status=429)
    else:
        resp = Response('OK', status=200)

    resp.headers['X-Rate-Limit-Hour-Remaining'] = CALL_PER_HOUR - hour_score
    resp.headers['X-Rate-Limit-Day-Remaining'] = CALL_PER_DAY - day_score
    return resp


if __name__ == '__main__':
    app.run()
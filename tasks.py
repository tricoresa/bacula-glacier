from celery import Celery

app = Celery('tasks', backend='amqp', broker='amqp://')
@app.task
def add(x, y):
    return x + y


@app.task
def mul(x, y):
    return x * y


@app.task
def xsum(numbers):
    return sum(numbers)


@app.task
def gen_prime(x):
    multiples = []
    results = []
    for i in xrange(2, x + 1):
        if i not in multiples:
            results.append(i)
            for j in xrange(i * i, x + 1, i):
                multiples.append(j)
    return results

import time

from celery.app import App


app = App()


@app.task
def hello():
    time.sleep(5)
    return "hello world"

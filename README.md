简化版的celery，只提供最基本的任务消费功能。没有做任务结果持久化处理，只是将其打印在屏幕上。

# 功能：

支持线程并发模式和并发数命令行配置

支持异步消息消费

# 使用

```shell
# start redis-server first

poetry init
poetry shell
export PYTHONPATH=.
python bin/worker.py --pool threads --concurrency 10
python3 bin/worker.py --app example.tasks.tasks.apps --pool thread --concurrency 10

# another shell
poetry shell
export PYTHONPATH=.
python example/sender.py
```

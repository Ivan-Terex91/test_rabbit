import asyncio
import aiohttp
import json
# import pika
# import aiopika


async def task(rabbit_name_queue, freq_check, rabbit_settings):
    """Опрос очередей rabbitmq"""
    user = rabbit_settings['user']
    password = rabbit_settings['password']
    host = rabbit_settings['host']
    port = rabbit_settings['port']
    vhost = rabbit_settings['vhost']
    name_queue = rabbit_name_queue
    url = f"http://{host}:{port}/api/queues/{vhost}/{name_queue}"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        auth=aiohttp.BasicAuth(user, password),
                        url=url

                ) as response:
                    if response.status != 200:
                        raise Exception(
                            f"request to {url} returned with error; status code {response.status}, reason {response.reason}")
                    # тут логирование успешного или не успешного запроса
                    response = await response.json()

                consumer_count = response['consumers']
                message_count = response['messages']
                print(f"Опросили очередь {rabbit_name_queue}, с частотой опроса {freq_check}")
                print(f'consumer_count={consumer_count}, message_count={message_count}')
                #  тут логгирование какая очередь, количество консьюмеров и количество сообщений
        except Exception as e:
            print(e)
        await asyncio.sleep(freq_check)


async def main():
    """Точка входа"""

    list_rabbit_queues = []

    with open('config.json') as inf:
        settings = json.load(inf)

    # Берём настройки Rabbit
    rabbit_settings = settings['Rabbit']

    for queue in settings['Queues']:
        list_rabbit_queues.append((queue['name_queue'], int(queue['freq_check'])))

    # Создание списка задач на опрос очередей
    tasks = [asyncio.create_task(task(*rq, rabbit_settings)) for rq in list_rabbit_queues]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(main())

import asyncio
import aiohttp
import json
import logging.config
from logging_config import LOGGING_CONFIG
from math import ceil
import aioredis

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger('app_logger')


class CheckAndManagementScaling:
    """Класс вычисления потребности в масштабировании"""

    def __init__(self, queue_settings: dict, rabbit_settings: dict, redis_settings: dict):
        """
        :param queue_settings: параметры очереди
        :param rabbit_settings: настройки rabbitmq
        :param redis_settings: настройки redis
        """
        self.rabbit_name_queue = queue_settings.get('name_queue')
        self.min_consumers = queue_settings.get('min_quantity_consumers')
        self.max_consumers = queue_settings.get('max_quantity_consumers')
        self.default_consumers = queue_settings.get('default_quantity_consumers')
        self.freq_check = queue_settings.get('freq_check')
        ############
        self._max_allowed_time = queue_settings.get('max_allowed_time')
        self.max_allowed_time = self._max_allowed_time
        self.messages = 0
        # self.consumers = 0
        ############
        # Параметры redis
        self.redis_host = redis_settings.get('host', '127.0.0.1')
        self.redis_port = redis_settings.get('port', '6379')
        self.number_db = redis_settings.get('db', 0)
        self.namespace_redis = 'queue:'
        # Параметры rabbit
        self.user = rabbit_settings.get('user')
        self.password = rabbit_settings.get('password')
        self.host = rabbit_settings.get('host')
        self.port = rabbit_settings.get('port')
        self.vhost = rabbit_settings.get('vhost')
        self.url = f"http://{self.host}:{self.port}/api/queues/{self.vhost}/{self.rabbit_name_queue}"

    async def polling_length_queue_and_count_consumers(self):
        """Метод опроса длины очереди и количества консумеров подписанных на очередь"""
        while True:
            try:
                #  Может стоит перенести в main открывать сессию, а сюда передовать объект сессии
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            auth=aiohttp.BasicAuth(self.user, self.password),
                            url=self.url

                    ) as response:
                        if response.status != 200:
                            raise Exception(
                                f"request to {self.url} returned with error; status code {response.status}, reason {response.reason}")
                        response = await response.json()
                        # print(response['backing_queue_status'])
                    consumer_count = response.get('consumers')
                    message_count = response.get('messages')

                    ############
                    if message_count > self.messages:
                        self.max_allowed_time = self._max_allowed_time
                    self.messages = message_count
                    ############

                    avg_rate_consumers = response.get('backing_queue_status').get('avg_egress_rate')
                    avg_rate_producers = response.get('backing_queue_status').get('avg_ingress_rate')

                    await self.calculation_consumers(message_count, consumer_count, avg_rate_consumers,
                                                     avg_rate_producers)

                    logger.info(
                        msg=f'Polling {self.rabbit_name_queue}, consumer_count={consumer_count}, message_count={message_count}')

                    #  Тут отправляем recommended_consumer_count
            except Exception as e:
                # тут логируем ошибки
                logger.error(msg=e)

            await asyncio.sleep(self.freq_check)

    async def calculation_consumers(self, message_count, consumer_count, avg_rate_consumers,
                                    avg_rate_producers):
        """Метод рассчёта рекомендуемого количества консумеров"""
        if message_count == 0:
            self.max_allowed_time = self._max_allowed_time
            logger.info(
                f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count=0")
            return 0
        elif consumer_count == 0:
            self.max_allowed_time = self.max_allowed_time - self.freq_check
            if self.max_allowed_time < self._max_allowed_time / 2:
                logger.warning(
                    f'There are no consumers in the {self.rabbit_name_queue} queue, but the messages_count = {message_count}'
                )
                self.max_allowed_time = self._max_allowed_time / 2

            # Считаем количество консумеров необходимых для обработки сообщений в очереди в допустимое время
            # Надо предусматреть случай если они нулевые
            avg_speed_proc_cons, quantity_values_speed_proc_cons = await self.read_data_from_redis()

            # время выполнения задач существующими консумерами
            recommended_consumer_count = ceil((message_count / avg_speed_proc_cons) / self.max_allowed_time)

            logger.info(
                f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}"
            )
            return recommended_consumer_count

        else:
            self.max_allowed_time = self.max_allowed_time - self.freq_check

            #  Тут получается есть сообщения и есть консумеры значит можно обновлять статистику
            avg_speed_proc_cons, quantity_values_speed_proc_cons = await self.read_data_from_redis()
            # Пересчитываем
            sum_speed_cons_in_db = avg_speed_proc_cons * quantity_values_speed_proc_cons
            quantity_values_speed_proc_cons += 1
            # Берём ср скорость одного консумера из rabbit
            avg_speed_proc_cons_in_rabbit = avg_rate_consumers / consumer_count
            avg_speed_proc_cons = (sum_speed_cons_in_db + avg_speed_proc_cons_in_rabbit) / quantity_values_speed_proc_cons

            await self.write_data_in_redis(avg_speed_proc_cons, quantity_values_speed_proc_cons)

            # время выполнения задач существующими консумерами
            messages_execution_time = message_count / (avg_speed_proc_cons * consumer_count)

            if self.max_allowed_time < self._max_allowed_time / 2 and self.max_allowed_time < messages_execution_time:
                logger.warning(
                    f'There are only {consumer_count} consumers in the {self.rabbit_name_queue} queue, but the messages_count = {message_count}'
                )
                self.max_allowed_time = self._max_allowed_time / 2

            if messages_execution_time <= self.max_allowed_time:
                if avg_rate_producers < 0.001:
                    # Тут можно потихоньку схлопывать консумеры
                    # Считаем сколько нужно консумеров чтобы уложиться в допустимое время
                    required_speed_processing = message_count / self.max_allowed_time
                    recommended_consumer_count = ceil(required_speed_processing / avg_speed_proc_cons)
                    logger.info(
                        f"name_queue={self.rabbit_name_queue}, existing consumer count enough"
                    )
                    logger.info(
                        f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}"
                    )
                else:
                    # Не меняем количество консумеров
                    recommended_consumer_count = consumer_count
                    logger.info(
                        f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={consumer_count}"
                    )
                return recommended_consumer_count
            else:
                required_speed_processing = message_count / self.max_allowed_time
                recommended_consumer_count = ceil(required_speed_processing / avg_speed_proc_cons)

                if recommended_consumer_count > self.max_consumers:
                    logger.warning(
                        f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}, "
                        f"max_allowed_consumer_count={self.max_consumers}"
                    )
                    return self.max_consumers

                logger.info(
                    f"name_queue={self.rabbit_name_queue}, consumer_count={consumer_count}, recommended_consumer_count={recommended_consumer_count}"
                )
                return recommended_consumer_count

    async def init_redis(self):
        """Метод инициализации данных в redis"""
        red = await aioredis.create_redis(address=(self.redis_host, self.redis_port), encoding='utf8')
        name_queue = self.namespace_redis + self.rabbit_name_queue
        if not await red.exists(name_queue):
            await red.hmset_dict(
                name_queue,
                {"avg_speed_proc_cons": 1, "quantity_values_speed_proc_cons": 1, "avg_speed_start_cons": 0.0,
                 "quantity_values_speed_start_cons": 0}
            )

    async def read_data_from_redis(self):
        """Метод чтения данных и redis"""
        name_queue = self.namespace_redis + self.rabbit_name_queue
        conn = await aioredis.create_redis(address=(self.redis_host, self.redis_port), encoding='utf8')
        avg_speed_proc_cons = float(await conn.hget(key=name_queue, field='avg_speed_proc_cons'))
        quantity_values_speed_proc_cons = int(await conn.hget(key=name_queue,
                                                              field='quantity_values_speed_proc_cons'))
        return avg_speed_proc_cons, quantity_values_speed_proc_cons

    async def write_data_in_redis(self, avg_speed_proc_cons, quantity_values_speed_proc_cons):
        """Метод записи данных редис"""
        name_queue = self.namespace_redis + self.rabbit_name_queue
        conn = await aioredis.create_redis(address=(self.redis_host, self.redis_port), encoding='utf8')
        await conn.hset(key=name_queue, field='avg_speed_proc_cons', value=avg_speed_proc_cons)
        await conn.hset(key=name_queue, field='quantity_values_speed_proc_cons',
                        value=quantity_values_speed_proc_cons)

    async def __call__(self, *args, **kwargs):
        await self.init_redis()
        await self.polling_length_queue_and_count_consumers()


async def main():
    """Точка входа"""

    with open('test_config.json') as inf:
        settings = json.load(inf)

    # Берём настройки Rabbit и redis
    rabbit_settings = settings.get('Rabbit')
    redis_settings = settings.get('Redis')
    # Список экземпляров классов check_scaling (очередь-экземпляр)
    list_rabbit_queues = []

    for queue_settings in settings.get('Queues'):
        list_rabbit_queues.append(
            CheckAndManagementScaling(queue_settings, rabbit_settings, redis_settings)
        )

    # Создание списка задач на вычисления потребности в масштабировании
    tasks = [asyncio.create_task(inst_check_scaling()) for inst_check_scaling in list_rabbit_queues]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    # logging.config.dictConfig(LOGGING_CONFIG)
    # logger = logging.getLogger('app_logger')
    asyncio.run(main())
